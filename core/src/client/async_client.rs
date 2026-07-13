use std::time::Duration;

use arcstr::ArcStr;
use bytes::Bytes;
use tokio::sync::watch;
use tokio::time;
use tracing::{debug, error, info, warn};

use super::config::MqttClientConfig;
use super::error::MqttClientError;
use super::publisher::MqttPublisher;
use super::subscriber::MqttSubscriber;
use crate::client::error::{ConnectReasonCode, ConnectionEstablishmentError};
use crate::connection::MqttConnection;
use crate::connection_state::{ConnectionState, DisconnectReason};
use crate::message_meta::RawMeta;
use crate::message_serializer::MessageSerializer;
use crate::routing::subscription_manager::SubscriptionConfig;
use crate::routing::{SubscriptionManagerActor, SubscriptionManagerHandler};
use crate::rumqttc;
use crate::rumqttc::Packet::{self, Publish};
use crate::rumqttc::{AsyncClient, ConnectReturnCode, EventLoop};
use crate::rumqttc::{Event::Incoming, Event::Outgoing};
use crate::topic::{TopicError, TopicPatternPath};

/// Type-safe MQTT client with automatic subscription management.
///
/// Provides typed publishers and subscribers with automatic serialization.
/// Connection lifecycle is managed separately via `MqttConnection`.
#[derive(Clone, Debug)]
pub struct MqttClient<F> {
	client: AsyncClient,
	subscription_manager_handler: SubscriptionManagerHandler<Bytes>,
	serializer: F,
	/// Seed handle for the connection-state watch channel. This handle is NEVER
	/// polled (`changed()`/`borrow_and_update()`), so it stays frozen at the
	/// initial watch version: every `connection_state()` clone inherits that
	/// frozen version, so a task subscribing AFTER a terminal transition still
	/// observes `Disconnected` on its first read. Advancing this handle would
	/// make late subscribers miss the terminal state.
	state_rx: watch::Receiver<ConnectionState>,
}

/// A successfully bootstrapped connection: the polled event loop plus the
/// `session_present` flag from its CONNACK (used to seed the initial
/// `ConnectionState::Connected`).
struct Established {
	event_loop: EventLoop,
	session_present: bool,
}

impl<F> MqttClient<F>
where F: Default + Clone + Send + Sync + 'static
{
	/// Create MQTT client with default configuration.
	///
	/// Returns both client and connection handle. Keep connection alive
	/// for the session duration, call `connection.shutdown()` when done.
	pub async fn connect(
		url: &str,
	) -> Result<(Self, MqttConnection), MqttClientError> {
		let config = MqttClientConfig::<F>::from_url(url)?;
		Self::connect_with_config(config).await
	}

	/// Create a new MQTT client with custom configuration
	pub async fn connect_with_config(
		config: MqttClientConfig<F>,
	) -> Result<(Self, MqttConnection), MqttClientError> {
		let topic_path_cache_capacity =
			std::num::NonZeroUsize::new(config.settings.topic_cache_size)
				.ok_or_else(|| {
					MqttClientError::ConfigurationValue(
						"topic_cache_size must be greater than 0".to_string(),
					)
				})?;
		// Single facade -> backend conversion point (validates instead of
		// inheriting rumqttc's assert-panics).
		let backend_options = config.connection.to_backend()?;
		let (client, new_event_loop) = AsyncClient::builder(backend_options)
			.capacity(config.settings.event_loop_capacity)
			.try_build()
			.map_err(|err| {
				MqttClientError::ConfigurationValue(err.to_string())
			})?;

		let timeout_millis = config.settings.connection_timeout_millis;
		let connection_timeout = Duration::from_millis(timeout_millis);
		let Established {
			event_loop: connected_event_loop,
			session_present,
		} = tokio::time::timeout(
			connection_timeout,
			Self::establish_connection(new_event_loop),
		)
		.await
		.map_err(|_| ConnectionEstablishmentError::Timeout { timeout_millis })?
		.map_err(MqttClientError::ConnectionEstablishment)?;

		let (controller, handler) = SubscriptionManagerActor::spawn(
			client.clone(),
			topic_path_cache_capacity,
			config.settings.command_channel_capacity,
			config.settings.unsubscribe_channel_capacity,
		);

		// Seed the connection-state channel from the bootstrap CONNACK. `connect`
		// only returns after a successful CONNACK, so `Connected` is always the
		// correct initial state.
		let (state_tx, state_rx) =
			watch::channel(ConnectionState::Connected { session_present });

		// Spawn the event loop in a separate task to handle MQTT messages
		// The event loop will terminate when it receives a Disconnect packet
		let handler_clone = handler.clone();
		let event_loop_handle = tokio::spawn(async move {
			Self::run(connected_event_loop, handler_clone, state_tx).await;
		});
		let fresh_client = Self {
			client: client.clone(),
			subscription_manager_handler: handler,
			serializer: F::default(),
			state_rx,
		};
		let connection =
			MqttConnection::new(client, controller, event_loop_handle);
		Ok((fresh_client, connection))
	}

	async fn establish_connection(
		mut event_loop: EventLoop,
	) -> Result<Established, ConnectionEstablishmentError> {
		loop {
			match event_loop.poll().await {
				| Ok(Incoming(Packet::ConnAck(connack))) => {
					let code = connack.code;
					let session_present = connack.session_present;
					if code == ConnectReturnCode::Success {
						debug!("MQTT connection established successfully");
						return Ok(Established {
							event_loop,
							session_present,
						});
					}
					debug!(code = ?code, "MQTT connection rejected by broker");
					return Err(ConnectionEstablishmentError::BrokerRejected {
						code: ConnectReasonCode::from_backend(code),
					});
				}
				| Ok(notification) => {
					debug!(notification = ?notification, "Bootstrap phase notification");
				}
				| Err(connection_err) => {
					debug!(error = %connection_err, "MQTT connection error during bootstrap phase");
					return Err(ConnectionEstablishmentError::from_backend(
						connection_err,
					));
				}
			}
		}
	}

	/// Main event loop that processes MQTT messages and handles graceful shutdown
	/// The loop terminates naturally when receiving a Disconnect packet (Incoming or Outgoing)
	async fn run(
		mut event_loop: EventLoop,
		subscription_manager: SubscriptionManagerHandler<Bytes>,
		state_tx: watch::Sender<ConnectionState>,
	) {
		// Two distinct counters, deliberately NOT merged:
		//  - `error_count` drives the exponential backoff and the
		//    `MAX_CONSECUTIVE_ERRORS` termination. It resets only on real
		//    progress (a delivered message or other notification), NOT on a bare
		//    reconnect CONNACK — so a broker that keeps accepting then dropping
		//    the connection still accumulates toward termination with a growing
		//    backoff, instead of storming reconnects at the 100ms floor forever.
		//  - `reconnect_attempt` is the observable `Reconnecting { attempt }`
		//    value: consecutive poll failures since the last successful poll. It
		//    resets on ANY success (reconnect, message, or notification).
		let mut error_count = 0;
		let mut reconnect_attempt = 0;
		const MAX_CONSECUTIVE_ERRORS: u32 = 10;
		const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(100);
		const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

		// Main processing loop - continues until Disconnect packet is received.
		// Each `break` yields the terminal `DisconnectReason`, published once
		// after the loop (single exit point, mirroring the single cleanup trigger).
		// State publishes are best-effort: `send` only errors once every
		// `MqttClient` (holding a receiver) is dropped.
		let reason = loop {
			match event_loop.poll().await {
				| Ok(Incoming(Packet::ConnAck(connack))) => {
					if connack.code == ConnectReturnCode::Success {
						reconnect_attempt = 0;
						if connack.session_present {
							info!(
								"MQTT reconnected with session preserved, \
								 subscriptions maintained by broker"
							);
						} else {
							info!(
								"MQTT reconnected without session, \
								 resubscribing to all topics"
							);
							if let Err(err) =
								subscription_manager.resubscribe_all().await
							{
								error!(error = ?err, "Failed to resubscribe to topics");
							}
						}
						drop(state_tx.send(ConnectionState::Connected {
							session_present: connack.session_present,
						}));
					} else {
						debug!(
							code = ?connack.code,
							"MQTT reconnection rejected by broker"
						);
					}
				}
				| Ok(Incoming(Publish(p))) => {
					// A delivered message is real progress: reset both counters.
					error_count = 0;
					reconnect_attempt = 0;

					let topic = String::from_utf8_lossy(&p.topic).into_owned();
					debug!(topic = %topic, payload_size = p.payload.len(), "Received MQTT message");

					let meta = RawMeta {
						qos: p.qos.into(),
						retain: p.retain,
						dup: p.dup,
					};
					if let Err(err) = subscription_manager
						.dispatch_incoming_message(topic, meta, p.payload)
						.await
					{
						error!(error = ?err, "Failed to send data to subscription manager");
					}
				}
				#[cfg(feature = "rumqttc-v4")]
				| Ok(Incoming(Packet::Disconnect)) => {
					info!("Received MQTT Disconnect packet from server");
					// Server initiated disconnect - terminate gracefully
					break DisconnectReason::BrokerDisconnected {};
				}
				#[cfg(all(
					feature = "rumqttc-v5",
					not(feature = "rumqttc-v4")
				))]
				| Ok(Incoming(Packet::Disconnect(_))) => {
					info!("Received MQTT Disconnect packet from server");
					// Server initiated disconnect - terminate gracefully
					break DisconnectReason::BrokerDisconnected {};
				}
				| Ok(Outgoing(rumqttc::Outgoing::Disconnect)) => {
					info!("Sent MQTT Disconnect packet to server");
					// Client initiated disconnect (via shutdown()) - terminate gracefully
					break DisconnectReason::CleanShutdown;
				}
				| Ok(notification) => {
					// A successful poll: reset both counters.
					error_count = 0;
					reconnect_attempt = 0;
					debug!(notification = ?notification, "Received OTHER MQTT notification");
				}
				| Err(err) => {
					error_count += 1;
					reconnect_attempt += 1;
					error!(error_count = error_count, error = %err, "MQTT event loop error");

					if error_count >= MAX_CONSECUTIVE_ERRORS {
						error!(
							error_count = error_count,
							max_errors = MAX_CONSECUTIVE_ERRORS,
							"Too many consecutive errors, terminating event \
							 loop"
						);
						break DisconnectReason::MaxErrorsExceeded {
							errors: error_count,
						};
					}

					drop(state_tx.send(ConnectionState::Reconnecting {
						attempt: reconnect_attempt,
					}));

					// Exponential backoff with jitter
					let delay = INITIAL_RETRY_DELAY
						* 2_u32.pow((error_count - 1).min(10));
					let delay = delay.min(MAX_RETRY_DELAY);

					warn!(delay = ?delay, error_count = error_count, "Retrying MQTT connection");
					time::sleep(delay).await;
				}
			}
		};
		info!("MQTT event loop terminated gracefully");
		// Event loop naturally terminated after receiving Disconnect packet
		// This ensures all MQTT messages were properly processed before shutdown

		// Publish the terminal state BEFORE closing the message streams, so a
		// task watching `connection_state()` can react before its `receive()`
		// starts yielding `None`.
		drop(state_tx.send(ConnectionState::Disconnected { reason }));

		// Terminal death (any of the three break paths above) must close the
		// subscriber channels, otherwise every consumer parks on `receive().await`
		// forever. On explicit `MqttConnection::shutdown()` the controller already
		// tore the actor down, so this is a harmless no-op on a closed channel.
		subscription_manager.shutdown().await;
	}

	/// Create typed publisher for specific topic.
	///
	/// Topic must not contain wildcard characters (`+`, `#`).
	pub fn get_publisher<T>(
		&self,
		topic: impl Into<ArcStr>,
	) -> Result<MqttPublisher<T, F>, TopicError>
	where
		T: Sync,
		F: MessageSerializer<T>,
	{
		let topic = topic.into();
		//Add type illegal topic
		validate_mqtt_topic(topic.as_str())?;
		Ok(MqttPublisher::new(
			self.client.clone(),
			self.serializer.clone(),
			topic,
		))
	}

	/// Subscribe to topic pattern with default configuration.
	///
	/// Supports MQTT wildcards: `+` (single level), `#` (multi-level).
	pub async fn subscribe<T>(
		&self,
		topic: impl TryInto<TopicPatternPath, Error: Into<MqttClientError>>,
	) -> Result<MqttSubscriber<T, F>, MqttClientError>
	where
		T: 'static + Send + Sync,
		F: MessageSerializer<T>,
	{
		self.subscribe_with_config(topic, SubscriptionConfig::default())
			.await
	}

	/// Subscribe with custom configuration (`QoS`, caching strategy)
	pub async fn subscribe_with_config<T>(
		&self,
		topic: impl TryInto<TopicPatternPath, Error: Into<MqttClientError>>,
		config: SubscriptionConfig,
	) -> Result<MqttSubscriber<T, F>, MqttClientError>
	where
		T: 'static + Send + Sync,
		F: MessageSerializer<T>,
		//TP: TryInto<TopicPatternPath>,
		//TP::Error: Into<MqttClientError>
	{
		let topic_pattern = topic.try_into().map_err(Into::into)?;
		//TopicPatternPath::new_from_string(topic, config.cache_strategy)?;
		let subscriber = self
			.subscription_manager_handler
			.create_subscription(topic_pattern, config)
			.await?;
		Ok(MqttSubscriber::new(subscriber, self.serializer.clone()))
	}
}

// Separate impl block for serializer transformation methods
// These don't require F to have Default/Send/Sync bounds
impl<F> MqttClient<F> {
	/// Clone client with a different serializer type.
	///
	/// This creates a new client instance that shares the same underlying
	/// MQTT connection and subscription manager, but uses a different
	/// serializer for message encoding/decoding.
	///
	/// This is a lightweight operation - the underlying MQTT connection
	/// (`AsyncClient`) and subscription manager are reference-counted and
	/// shared between instances.
	///
	/// # Type Parameters
	///
	/// * `S` - The new serializer type, must implement `Default`
	///
	/// # Example
	///
	/// ```rust,ignore
	/// use mqtt_typed_client_core::{MqttClient, WincodeSerializer};
	/// use serde::{Deserialize, Serialize};
	/// use wincode::{SchemaWrite, SchemaRead};
	///
	/// #[derive(Serialize, Deserialize, SchemaWrite, SchemaRead)]
	/// struct LegacyData { value: f64 }
	///
	/// #[derive(Serialize, Deserialize, SchemaWrite, SchemaRead)]
	/// struct ModernData { value: f64 }
	///
	/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
	/// // Connect with default Wincode serializer
	/// let (client, connection) = MqttClient::<WincodeSerializer>::connect("mqtt://localhost?client_id=doc_client").await?;
	///
	/// // Clone into another default-constructed serializer instance
	/// let cloned_client = client.clone_with_serializer::<WincodeSerializer>();
	/// let legacy_sub = cloned_client.subscribe::<LegacyData>("legacy/sensors/+").await?;
	///
	/// // Original client with Wincode still usable
	/// let modern_sub = client.subscribe::<ModernData>("v2/sensors/+").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn clone_with_serializer<S>(&self) -> MqttClient<S>
	where S: Default + Clone + Send + Sync + 'static {
		self.clone_with_custom_serializer(S::default())
	}

	/// Clone client with a custom-configured serializer instance.
	///
	/// Use this method when you need a preconfigured serializer instance,
	/// such as a custom application serializer with runtime settings.
	///
	/// This is a lightweight operation - the underlying MQTT connection
	/// and subscription manager are shared between instances.
	///
	/// # Arguments
	///
	/// * `serializer` - A configured serializer instance
	///
	/// # Example
	///
	/// ```rust,ignore
	/// use mqtt_typed_client_core::{MqttClient, WincodeSerializer};
	/// use serde::{Deserialize, Serialize};
	/// use wincode::{SchemaWrite, SchemaRead};
	///
	/// #[derive(Serialize, Deserialize, SchemaWrite, SchemaRead)]
	/// struct Data { value: f64 }
	///
	/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
	/// let (client, connection) = MqttClient::<WincodeSerializer>::connect("mqtt://localhost?client_id=doc_client").await?;
	///
	/// // Use an explicitly constructed serializer instance
	/// let custom_client = client.clone_with_custom_serializer(WincodeSerializer::default());
	/// let publisher = custom_client.get_publisher::<Data>("topic/with/custom/encoding")?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn clone_with_custom_serializer<S>(
		&self,
		serializer: S,
	) -> MqttClient<S>
	where
		S: Clone + Send + Sync + 'static,
	{
		MqttClient {
			client: self.client.clone(),
			subscription_manager_handler: self
				.subscription_manager_handler
				.clone(),
			serializer,
			// Serializer-swapped client shares the same connection, so it shares
			// the same connection-state channel.
			state_rx: self.state_rx.clone(),
		}
	}

	/// Watch the connection lifecycle.
	///
	/// Returns an independent [`watch::Receiver`] pre-seeded with the current
	/// [`ConnectionState`]; all clones observe the same channel. `Disconnected`
	/// is **terminal** — once observed, `changed()` never fires again, so prefer
	/// a `loop { rx.changed().await?; if matches!(*rx.borrow(),
	/// ConnectionState::Disconnected { .. }) { break } }` shape over spinning on
	/// `changed()`.
	pub fn connection_state(&self) -> watch::Receiver<ConnectionState> {
		self.state_rx.clone()
	}
}

fn validate_mqtt_topic(topic_str: &str) -> Result<(), TopicError> {
	//let topic_str = topic.as_ref();
	if topic_str.is_empty() || topic_str.len() > 65535 {
		return Err(crate::topic::TopicRouterError::invalid_routing_topic(
			topic_str,
			"Topic is empty or too long",
		)
		.into());
	}
	if topic_str.chars().any(|c| matches!(c, '\0' | '#' | '+')) {
		return Err(crate::topic::TopicRouterError::invalid_routing_topic(
			topic_str,
			"Topic contains illegal characters ('#', '+', or null byte)",
		)
		.into());
	}
	Ok(())
}
