//! Typed topic wrapper example for `rumqttc-v4-next`.
//!
//! Demonstrates the typed client with automatic topic routing and JSON
//! serialization over MQTT 3.1.1.
//!
//! ```sh
//! cargo run --features "rumqttc-v4,macros,json" --example rumqttc_v4_typed_client
//! ```

use mqtt_typed_client::{JsonSerializer, MqttClient};
use mqtt_typed_client_macros::mqtt_topic;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::io;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Reading {
    temperature_c: f32,
    battery_percent: u8,
}

#[mqtt_topic("rumqtt/v4/{room}/{sensor_id}/reading")]
struct SensorReading {
    room: String,
    sensor_id: u32,
    payload: Reading,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let broker_url = std::env::var("MQTT_BROKER_URL")
        .unwrap_or_else(|_| "mqtt://localhost:1883?client_id=rumqtt-v4-typed-example".to_owned());

    println!("connecting to {broker_url}");
    let (client, connection) = MqttClient::<JsonSerializer>::connect(&broker_url).await?;

    let topic_client = client.sensor_reading();
    let mut subscriber = topic_client.subscribe().await?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    let reading = Reading {
        temperature_c: 22.5,
        battery_percent: 91,
    };

    topic_client.publish("lab", 7, &reading).await?;

    let received = tokio::time::timeout(Duration::from_secs(3), subscriber.receive()).await?;
    match received {
        Some(Ok(message)) => {
            println!(
                "received room={} sensor_id={} reading={:?}",
                message.room, message.sensor_id, message.payload
            );
        }
        Some(Err(error)) => return Err(Box::<dyn Error>::from(error)),
        None => {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "subscriber closed before receiving message",
            )));
        }
    }

    connection.shutdown().await?;
    Ok(())
}
