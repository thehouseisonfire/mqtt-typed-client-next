//! # Hello World - Custom Serializers
//!
//! Demonstrates different message serialization approaches:
//! 1. Using MessagePackSerializer (custom binary format)
//! 2. Shows how to implement MessageSerializer trait

mod shared;

use mqtt_typed_client::{MessageSerializer, MqttClient};
use mqtt_typed_client_macros::mqtt_topic;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    text: String,
    timestamp: u64,
}

#[mqtt_topic("greetings/{language}/{sender}")]
pub struct GreetingTopic {
    language: String,
    sender: String,
    payload: Message,
}

#[derive(Clone, Default)]
pub struct MessagePackSerializer;

impl MessagePackSerializer {
    pub fn new() -> Self {
        Self
    }
}

impl<T> MessageSerializer<T> for MessagePackSerializer
where
    T: serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    type SerializeError = rmp_serde::encode::Error;
    type DeserializeError = rmp_serde::decode::Error;

    fn serialize(&self, data: &T) -> Result<Vec<u8>, Self::SerializeError> {
        rmp_serde::to_vec(data)
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<T, Self::DeserializeError> {
        rmp_serde::from_slice(bytes)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    shared::tracing::setup(None);

    println!("Starting MQTT serializer demonstration...\n");

    let connection_url = shared::config::build_url("serializers_demo");

    let (client, connection) = MqttClient::<MessagePackSerializer>::connect(&connection_url)
        .await
        .inspect_err(|e| {
            shared::config::print_connection_error(&connection_url, e);
        })?;

    println!("Connected to MQTT broker");

    let topic_client = client.greeting_topic();
    let mut subscriber = topic_client.subscribe().await?;
    println!("Subscribed to: greetings/+/+");

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let message = Message {
        text: "Hello world!".to_string(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    };

    println!("Publishing message to: greetings/msgpack/demo");
    topic_client.publish("msgpack", "demo", &message).await?;

    println!("Waiting for message...");
    if let Some(Ok(greeting)) = subscriber.receive().await {
        println!(
            "Received from {}/{}: {} (timestamp: {})",
            greeting.language, greeting.sender, greeting.payload.text, greeting.payload.timestamp
        );
    }

    connection.shutdown().await?;
    println!("Disconnected");

    Ok(())
}
