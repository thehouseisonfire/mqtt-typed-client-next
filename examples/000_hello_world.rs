//! # Hello World - MQTT Typed Client
//!
//! A simple example demonstrating key features:
//! - Automatic topic parameter parsing with `#[mqtt_topic]` macro
//! - Type-safe message routing
//! - Automatic serialization/deserialization

mod shared;

use mqtt_typed_client::{MqttClient, WincodeSerializer};
use mqtt_typed_client_macros::mqtt_topic;
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

#[derive(Serialize, Deserialize, SchemaWrite, SchemaRead, Debug)]
struct Message {
    text: String,
}

#[mqtt_topic("greetings/{language}/{sender}")]
pub struct GreetingTopic {
    language: String,
    sender: String,
    payload: Message,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    shared::tracing::setup(None);

    println!("Starting MQTT Hello World example...\n");

    let connection_url = shared::config::build_url("hello_world");
    println!("Connecting to MQTT broker: {connection_url}");

    let (client, connection) = MqttClient::<WincodeSerializer>::connect(&connection_url)
        .await
        .inspect_err(|e| {
            shared::config::print_connection_error(&connection_url, e);
        })?;

    println!("Connected to MQTT broker");

    let topic_client = client.greeting_topic();
    let mut subscriber = topic_client.subscribe().await?;
    println!("Subscribed to: greetings/+/+");

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let hello_message = Message {
        text: "Hello, World!".to_string(),
    };

    println!("Publishing greeting message to topic: greetings/rust/rustacean");
    topic_client
        .publish("rust", "rustacean", &hello_message)
        .await?;

    println!("Waiting for greeting message from broker...");
    if let Some(Ok(greeting)) = subscriber.receive().await {
        println!("Received greeting:");
        println!("   Language: {}", greeting.language);
        println!("   Sender: {}", greeting.sender);
        println!("   Message: {}", greeting.payload.text);
    }

    connection.shutdown().await?;
    println!("\nGoodbye!");

    Ok(())
}
