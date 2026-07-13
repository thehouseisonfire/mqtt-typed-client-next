//! Typed client example using MQTT 5 CONNECT properties.
//!
//! Demonstrates passing MQTT 5 CONNECT properties through the typed client
//! config.
//!
//! ```sh
//! cargo run --features "rumqttc-v5,macros,json,unstable-backend-api" --example rumqttc_v5_connect_properties
//! ```

use std::error::Error;
use std::io;
use std::time::Duration;

use mqtt_typed_client::backend::{
	self, rumqttc::mqttbytes::v5::ConnectProperties,
};
use mqtt_typed_client::{
	JsonSerializer, MqttClient, MqttClientConfig, ReceiveEvent,
};
use mqtt_typed_client_macros::mqtt_topic;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Reading {
	humidity_percent: u8,
}

#[mqtt_topic("rumqtt/v5/connect/{site}/{sensor_id}/reading")]
struct ConnectPropertyReading {
	site: String,
	sensor_id: u32,
	payload: Reading,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
	let mut connect_properties = ConnectProperties::new();
	connect_properties.session_expiry_interval = Some(60);
	connect_properties.receive_maximum = Some(16);
	connect_properties.topic_alias_max = Some(8);
	connect_properties.user_properties =
		vec![("client-kind".into(), "typed-v5-example".into())];

	let mut config = MqttClientConfig::<JsonSerializer>::new(
		"rumqtt-v5-typed-connect-properties",
		"localhost",
		1883,
	);
	config.connection.keep_alive = Duration::from_secs(5);
	config.connection.backend_tweak(move |options| {
		if let backend::BackendOptions::V5(options) = options {
			options.set_connect_properties(connect_properties.clone());
		}
	});

	let (client, connection) =
		MqttClient::<JsonSerializer>::connect_with_config(config).await?;

	let topic_client = client.connect_property_reading();
	let mut subscriber = topic_client.subscribe().await?;

	tokio::time::sleep(Duration::from_millis(300)).await;

	topic_client
		.publish(
			"lab",
			42,
			&Reading {
				humidity_percent: 64,
			},
		)
		.await?;

	let received =
		tokio::time::timeout(Duration::from_secs(3), subscriber.receive())
			.await?;
	match received {
		| Some(ReceiveEvent::Message(message)) => {
			println!(
				"received site={} sensor_id={} reading={:?}",
				message.site, message.sensor_id, message.payload
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
