//! # Hello World with Last Will & Testament (LWT)
//!
//! Demonstrates MQTT Last Will & Testament functionality.
//!
//! Usage:
//! ```bash
//! # Terminal 1: Start subscriber
//! cargo run --example 003_hello_world_lwt
//!
//! # Terminal 2: Run publisher (sends message then crashes)
//! cargo run --example 003_hello_world_lwt -- --publisher
//! ```

mod shared;

use std::env;

use mqtt_typed_client::{MqttClient, MqttClientConfig, QoS, WincodeSerializer};
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

    let args: Vec<String> = env::args().collect();
    let is_publisher = args.len() > 1 && args[1] == "--publisher";

    if is_publisher {
        run_publisher().await
    } else {
        run_subscriber().await
    }
}

async fn run_subscriber() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting MQTT Subscriber for LWT demo...");
    println!("\nIn another terminal, run:");
    println!("   cargo run --example 003_hello_world_lwt -- --publisher\n");

    let connection_url = shared::config::build_url("lwt_subscriber");
    println!("Connecting subscriber to MQTT broker: {connection_url}");

    let (client, connection) = MqttClient::<WincodeSerializer>::connect(&connection_url)
        .await
        .inspect_err(|e| {
            shared::config::print_connection_error(&connection_url, e);
        })?;

    println!("Subscriber connected to MQTT broker");

    let topic_client = client.greeting_topic();
    let mut subscriber = topic_client.subscribe().await?;

    println!("Subscribed to: greetings/+/+ (will receive both normal and LWT messages)");

    println!("Waiting for messages... (Press Ctrl+C to exit)\n");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("\nSubscriber shutting down...");
        },
        _ = async {
            let mut message_count = 0;
            while let Some(result) = subscriber.receive().await {
                match result {
                    Ok(greeting) => {
                        message_count += 1;
                        if greeting.payload.text.contains("LWT") {
                            println!("[{}] LWT from {}/{}: {} (publisher disconnected unexpectedly)",
                                message_count, greeting.language, greeting.sender, greeting.payload.text);
                        } else {
                            println!("[{}] Greeting from {}/{}: {}",
                                message_count, greeting.language, greeting.sender, greeting.payload.text);
                        }
                    },
                    Err(e) => {
                        eprintln!("Error receiving message: {e}");
                    }
                }
            }
        } => {}
    };

    connection.shutdown().await?;
    println!("Subscriber disconnected gracefully\n");
    Ok(())
}

async fn run_publisher() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting MQTT Publisher for LWT demo...");

    let connection_url = shared::config::build_url("lwt_publisher");
    println!("Connecting publisher to MQTT broker: {connection_url}");

    let mut config = MqttClientConfig::<WincodeSerializer>::from_url(&connection_url)?;

    let lwt_message = Message {
        text: "Bye bye LWT!".to_string(),
    };

    let last_will =
        GreetingTopic::last_will("rust", "publisher", lwt_message).qos(QoS::AtLeastOnce);

    config.with_last_will(last_will)?;

    println!("LWT configured: 'Bye bye LWT!' on topic greetings/rust/publisher");

    let (client, _connection) = MqttClient::connect_with_config(config)
        .await
        .inspect_err(|e| {
            shared::config::print_connection_error(&connection_url, e);
        })?;

    println!("Publisher connected with LWT configured");

    let topic_client = client.greeting_topic();

    let hello_message = Message {
        text: "Hello, World!".to_string(),
    };

    println!("Publishing greeting message...");
    topic_client
        .publish("rust", "publisher", &hello_message)
        .await?;

    println!("Greeting sent successfully!");

    println!("\nSimulating unexpected disconnect in 2 seconds...");
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    println!("Publisher crashing now! (LWT should be triggered)");
    std::process::exit(0);
}
