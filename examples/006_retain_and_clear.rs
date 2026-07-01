//! # MQTT Retain & Clear Messages Demo
//!
//! Demonstrates MQTT retained message functionality with multiple clients
//! connecting at different times to showcase how retained messages work.
//!
//! ## Demo Timeline (20 seconds total):
//!
//! ```text
//! t=0s:  Publisher sends retained message #1
//! t=1s:  Subscriber-1 connects -> receives retained message #1
//! t=5s:  Publisher sends retained message #2 (replaces #1)
//! t=6s:  Subscriber-2 connects -> receives retained message #2
//! t=10s: Publisher sends non-retained message #3
//! t=11s: Subscriber-3 connects -> receives retained message #2 (not #3)
//! t=15s: Publisher clears retained message
//! t=18s: Subscriber-4 connects -> receives nothing
//! ```

mod shared;

use std::time::Duration;

use mqtt_typed_client::{MqttClient, MqttClientError, MqttConnection, WincodeSerializer};
use mqtt_typed_client_macros::mqtt_topic;
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

#[derive(Serialize, Deserialize, SchemaWrite, SchemaRead, Debug, Clone)]
struct DemoMessage {
    content: String,
}

#[mqtt_topic("demo/retain")]
pub struct RetainDemoTopic {
    payload: DemoMessage,
}

async fn create_client(
    client_id: &str,
) -> Result<(MqttClient<WincodeSerializer>, MqttConnection), MqttClientError> {
    let connection_url = shared::config::build_url(client_id);
    MqttClient::<WincodeSerializer>::connect(&connection_url)
        .await
        .inspect_err(|e| shared::config::print_connection_error(&connection_url, e))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    shared::tracing::setup(None);

    println!("=== MQTT Retain & Clear Demo ===\n");
    println!("This demo shows how MQTT retained messages work with multiple clients.");
    println!("Watch how subscribers connecting at different times receive different messages.\n");

    tokio::join!(
        async { run_publisher().await.unwrap() },
        async { run_supervisor().await.unwrap() },
        async { run_delayed_subscriber(1, "subscriber-1").await.unwrap() },
        async { run_delayed_subscriber(6, "subscriber-2").await.unwrap() },
        async { run_delayed_subscriber(11, "subscriber-3").await.unwrap() },
        async { run_delayed_subscriber(18, "subscriber-4").await.unwrap() },
    );

    Ok(())
}

async fn run_publisher() -> Result<(), Box<dyn std::error::Error>> {
    println!("[PUBLISHER] Connecting to MQTT broker...");
    let (client, connection) = create_client("retain_publisher").await?;

    let topic_client = client.retain_demo_topic();
    let publisher = topic_client.get_publisher()?;

    println!("[PUBLISHER] Publishers configured, starting demo sequence...\n");

    tokio::time::sleep(Duration::from_secs(1)).await;

    let msg1 = DemoMessage {
        content: "Retained message #1: First stored message".to_string(),
    };
    println!("[PUBLISHER] t=0s: Publishing retained message #1");
    publisher.publish_retain(&msg1).await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let msg2 = DemoMessage {
        content: "Retained message #2: Updated stored message".to_string(),
    };
    println!("[PUBLISHER] t=5s: Publishing retained message #2 (replaces #1)");
    publisher.publish_retain(&msg2).await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let msg3 = DemoMessage {
        content: "Non-retained message #3: Temporary message".to_string(),
    };
    println!("[PUBLISHER] t=10s: Publishing non-retained message #3");
    publisher.publish_normal(&msg3).await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("[PUBLISHER] t=15s: Clearing retained message from broker");
    publisher.clear_retained().await?;

    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("[PUBLISHER] t=20s: Demo sequence completed\n");

    connection.shutdown().await?;
    Ok(())
}

async fn run_supervisor() -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = create_client("retain_supervisor").await?;

    let topic_client = client.retain_demo_topic();
    let mut subscriber = topic_client.subscribe().await?;
    println!("[SUPERVISOR] Started continuous monitoring of topic: demo/retain");
    println!("[SUPERVISOR] Will show all messages as they are published\n");

    let _result = tokio::time::timeout(Duration::from_secs(25), async {
        while let Some(result) = subscriber.receive().await {
            match result {
                Ok(msg) => {
                    let now = chrono::Utc::now().format("%H:%M:%S%.3f").to_string();
                    println!("[SUPERVISOR] {}: Received: '{}'", now, msg.payload.content);
                }
                Err(e) => {
                    eprintln!("[SUPERVISOR] Deserialization error: {e}");
                }
            }
        }
    })
    .await;

    connection.shutdown().await?;
    Ok(())
}

async fn run_delayed_subscriber(
    delay_seconds: u64,
    client_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::sleep(Duration::from_secs(delay_seconds)).await;

    let (client, connection) = create_client(&format!("retain_{client_id}")).await?;

    let topic_client = client.retain_demo_topic();
    let mut subscriber = topic_client.subscribe().await?;

    let now = chrono::Utc::now().format("%H:%M:%S%.3f").to_string();
    println!(
        "[{}] {}: Connected at t={}s, checking for retained messages...",
        client_id.to_uppercase(),
        now,
        delay_seconds
    );

    let timeout_result = tokio::time::timeout(Duration::from_secs(3), subscriber.receive()).await;

    match timeout_result {
        Ok(Some(Ok(msg))) => {
            let now = chrono::Utc::now().format("%H:%M:%S%.3f").to_string();
            println!(
                "[{}] {}: Received retained message: '{}'",
                client_id.to_uppercase(),
                now,
                msg.payload.content
            );
        }
        Ok(Some(Err(e))) => {
            println!(
                "[{}] Error receiving message: {}",
                client_id.to_uppercase(),
                e
            );
        }
        Ok(None) | Err(_) => {
            let now = chrono::Utc::now().format("%H:%M:%S%.3f").to_string();
            println!(
                "[{}] {}: No retained message received (broker storage empty)",
                client_id.to_uppercase(),
                now
            );
        }
    }

    connection.shutdown().await?;
    Ok(())
}
