//! # Custom Topic Patterns - MQTT Typed Client
//!
//! Demonstrates how to override default topic patterns while
//! maintaining type safety and parameter compatibility.

mod shared;

use mqtt_typed_client::{MqttClient, QoS, WincodeSerializer};
use mqtt_typed_client_macros::mqtt_topic;
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

#[derive(Serialize, Deserialize, SchemaWrite, SchemaRead, Debug, Clone)]
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

    println!("Starting Custom Topic Patterns example...\n");

    let connection_url = shared::config::build_url("custom_patterns");
    println!("Connecting to MQTT broker: {connection_url}");

    let (client, connection) = MqttClient::<WincodeSerializer>::connect(&connection_url)
        .await
        .inspect_err(|e| {
            shared::config::print_connection_error(&connection_url, e);
        })?;

    println!("Connected to MQTT broker");

    let topic_client = client.greeting_topic();

    println!("Default pattern: {}", GreetingTopic::TOPIC_PATTERN);
    println!("Default MQTT pattern: {}\n", GreetingTopic::MQTT_PATTERN);

    println!("Setting up custom subscription with environment prefix...");

    let mut custom_subscriber = topic_client
        .subscription()
        .with_pattern("dev/greetings/{language}/{sender}")?
        .subscribe()
        .await?;

    println!("Subscribed to: dev/greetings/+/+");

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let message = Message {
        text: "Hello from custom pattern!".to_string(),
    };

    println!("Publishing to custom pattern...");

    let custom_publisher =
        topic_client.get_publisher_to("dev/greetings/{language}/{sender}", "rust", "dev_user")?;

    custom_publisher.publish(&message).await?;
    println!("Published to: dev/greetings/rust/dev_user");

    let lwt_message = Message {
        text: "Client disconnected unexpectedly!".to_string(),
    };

    let custom_lwt = GreetingTopic::last_will_to(
        "dev/greetings/{language}/{sender}",
        "rust",
        "dev_client",
        lwt_message,
    )?
    .qos(QoS::AtLeastOnce);

    println!("Custom LWT topic: {}", custom_lwt.topic);

    println!("\nWaiting for published message...");

    if let Some(Ok(greeting)) = custom_subscriber.receive().await {
        println!("Received custom greeting:");
        println!("   Language: {}", greeting.language);
        println!("   Sender: {}", greeting.sender);
        println!("   Message: {}", greeting.payload.text);
    }

    connection.shutdown().await?;
    println!("\nGoodbye!");

    Ok(())
}
