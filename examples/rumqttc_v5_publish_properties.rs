//! Typed client example using MQTT 5 PUBLISH properties.
//!
//! The typed wrapper owns topic construction and serialization, while
//! `publish_with_options` passes explicit MQTT 5 publish properties through to
//! rumqttc.
//!
//! ```sh
//! cargo run --features "rumqttc-v5,macros,json,unstable-backend-api" --example rumqttc_v5_publish_properties
//! ```

use std::error::Error;
use std::io;
use std::time::Duration;

use mqtt_typed_client::backend::rumqttc::mqttbytes::v5::PublishProperties;
use mqtt_typed_client::backend::rumqttc::{PublishOptions, QoS};
use mqtt_typed_client::{JsonSerializer, MqttClient, ReceiveEvent};
use mqtt_typed_client_macros::mqtt_topic;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Status {
	online: bool,
	firmware: String,
}

#[mqtt_topic("rumqtt/v5/properties/{device_id}/status")]
struct DeviceStatus {
	device_id: String,
	payload: Status,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
	let broker_url =
		"mqtt://localhost:1883?client_id=rumqtt-v5-typed-publish-properties";
	let (client, connection) =
		MqttClient::<JsonSerializer>::connect(broker_url).await?;

	let topic_client = client.device_status();
	let mut subscriber = topic_client.subscribe().await?;

	tokio::time::sleep(Duration::from_millis(300)).await;

	let publisher = topic_client.get_publisher("device-17")?;
	let properties = PublishProperties {
		payload_format_indicator: Some(1),
		content_type: Some("application/json".into()),
		response_topic: Some("rumqtt/v5/properties/device-17/reply".into()),
		correlation_data: Some(bytes::Bytes::from_static(b"correlation-17")),
		user_properties: vec![("source".into(), "typed-client-next".into())],
		..Default::default()
	};

	publisher
		.publish_with_options(
			&Status {
				online: true,
				firmware: "5.0.1".into(),
			},
			PublishOptions::new(QoS::AtLeastOnce).properties(properties),
		)
		.await?;

	let received =
		tokio::time::timeout(Duration::from_secs(3), subscriber.receive())
			.await?;
	match received {
		| Some(ReceiveEvent::Message(message)) => {
			println!(
				"received device_id={} status={:?}",
				message.device_id, message.payload
			);
		}
		| Some(ReceiveEvent::DecodeFailed(error)) => {
			return Err(Box::<dyn Error>::from(error));
		}
		| Some(ReceiveEvent::Lagged { missed }) => {
			return Err(Box::new(io::Error::other(format!(
				"subscriber lagged and dropped {missed} messages"
			))));
		}
		| Some(_) => {
			return Err(Box::new(io::Error::other(
				"subscriber returned an unsupported receive event",
			)));
		}
		| None => {
			return Err(Box::new(io::Error::new(
				io::ErrorKind::UnexpectedEof,
				"subscriber closed before receiving message",
			)));
		}
	}

	connection.shutdown().await?;
	Ok(())
}
