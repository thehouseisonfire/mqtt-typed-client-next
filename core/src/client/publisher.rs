use std::marker::PhantomData;

use arcstr::ArcStr;
use mqtt_topic_engine::QoS;

use super::error::MqttClientError;
use crate::message_serializer::MessageSerializer;
use crate::rumqttc::{AsyncClient, PublishOptions};

/// Typed MQTT publisher for a specific topic.
///
/// Created via `MqttClient::get_publisher()`. Supports `QoS` and retain configuration.
pub struct MqttPublisher<T, F> {
	client: AsyncClient,
	topic: ArcStr,
	qos: QoS,
	retain: bool,
	serializer: F,
	_phantom: PhantomData<T>,
}

impl<T, F> MqttPublisher<T, F>
where
	T: Sync,
	F: MessageSerializer<T> + Sync,
{
	/// Internal constructor. Use `MqttClient::get_publisher()` instead.
	pub(crate) fn new(
		client: AsyncClient,
		serializer: F,
		topic: impl Into<ArcStr>,
	) -> Self {
		Self {
			client,
			topic: topic.into(),
			qos: QoS::AtLeastOnce,
			retain: false,
			serializer,
			_phantom: PhantomData,
		}
	}
	/// Sets Quality of Service level for published messages.
	#[must_use]
	pub const fn with_qos(mut self, qos: QoS) -> Self {
		self.qos = qos;
		self
	}

	/// Sets retain flag for published messages.
	#[must_use]
	pub const fn with_retain(mut self, retain: bool) -> Self {
		self.retain = retain;
		self
	}

	/// Get the topic this publisher is configured for.
	pub const fn topic(&self) -> &ArcStr {
		&self.topic
	}

	/// Get qos level for this publisher.
	pub const fn qos(&self) -> QoS {
		self.qos
	}

	/// Get retain flag for this publisher.
	pub const fn retain(&self) -> bool {
		self.retain
	}

	/// Publishes data to the configured topic.
	pub async fn publish(&self, data: &T) -> Result<(), MqttClientError> {
		self.publish_with_retain_override(data, self.retain).await
	}

	/// Publishes data using raw backend publish options.
	///
	/// This is part of the semver-exempt backend escape hatch and can change
	/// with backend crate changes.
	#[cfg(feature = "unstable-backend-api")]
	pub async fn publish_with_options(
		&self,
		data: &T,
		options: crate::client::backend::rumqttc::PublishOptions,
	) -> Result<(), MqttClientError> {
		let payload = self
			.serializer
			.serialize(data)
			.map_err(|e| MqttClientError::Serialization(format!("{e:?}")))?;
		self.client
			.publish(self.topic.as_str(), payload, options)
			.await
			.map_err(|err| MqttClientError::from_backend_client_error(&err))
	}

	/// Publishes data with retain flag explicitly set to true.
	pub async fn publish_retain(
		&self,
		data: &T,
	) -> Result<(), MqttClientError> {
		self.publish_with_retain_override(data, true).await
	}

	/// Publishes data with retain flag explicitly set to false.
	pub async fn publish_normal(
		&self,
		data: &T,
	) -> Result<(), MqttClientError> {
		self.publish_with_retain_override(data, false).await
	}

	/// Internal helper to avoid code duplication
	async fn publish_with_retain_override(
		&self,
		data: &T,
		retain: bool,
	) -> Result<(), MqttClientError> {
		let payload = self
			.serializer
			.serialize(data)
			.map_err(|e| MqttClientError::Serialization(format!("{e:?}")))?;
		self.client
			.publish(
				self.topic.as_str(),
				payload,
				publish_options(self.qos, retain),
			)
			.await
			.map_err(|err| MqttClientError::from_backend_client_error(&err))
	}

	/// Clear retained message for this topic
	///
	/// Sends an empty payload with retain=true to remove any retained message.
	/// Uses the same `QoS` level as configured for this publisher.
	pub async fn clear_retained(&self) -> Result<(), MqttClientError> {
		self.client
			.publish(
				self.topic.as_str(),
				Vec::new(),
				publish_options(self.qos, true),
			)
			.await
			.map_err(|err| MqttClientError::from_backend_client_error(&err))
	}
}

const fn publish_options(qos: QoS, retain: bool) -> PublishOptions {
	let options = PublishOptions::new(qos.to_rumqttc());
	if retain { options.retained() } else { options }
}
