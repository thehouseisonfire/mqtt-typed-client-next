//! # Hello World TLS - MQTT Typed Client
//!
//! Demonstrates secure MQTT connections with TLS/SSL:
//! - Custom CA certificate configuration
//! - TLS transport setup with rustls
//! - Self-signed certificate handling for development

mod shared;

use std::{fs, io::BufReader};

use mqtt_typed_client::rustls::{ClientConfig, RootCertStore};
use mqtt_typed_client::{MqttClient, MqttClientConfig, Transport, WincodeSerializer};
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

fn create_tls_config() -> Result<ClientConfig, Box<dyn std::error::Error>> {
    let mut root_cert_store = RootCertStore::empty();

    let ca_cert = fs::read("dev/certs/ca.pem")?;
    let mut reader = BufReader::new(&ca_cert[..]);

    let certs = rustls_pemfile::certs(&mut reader);
    for cert in certs {
        let cert = cert?;
        root_cert_store.add(cert)?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    Ok(config)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    shared::tracing::setup(None);

    println!("Starting MQTT Hello World TLS example...\n");

    let tls_config = create_tls_config()?;

    let client_id = shared::config::get_client_id("hello_world_tls");

    let mut config = MqttClientConfig::<WincodeSerializer>::new(&client_id, "localhost", 8883);

    config
        .connection
        .set_transport(Transport::tls_with_config(tls_config.into()));

    println!("Connecting to MQTT broker with TLS: localhost:8883");

    let (client, connection) = MqttClient::connect_with_config(config)
        .await
        .inspect_err(|e| {
            shared::config::print_connection_error("mqtts://localhost:8883", e);
            eprintln!();
            eprintln!("TLS-specific troubleshooting:");
            eprintln!("   - Ensure CA certificate exists: dev/certs/ca.pem");
            eprintln!("   - Check certificate permissions and format");
            eprintln!("   - Try plain MQTT: MQTT_BROKER=\"mqtt://localhost:1883\"");
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
