//! # Configuration Example - MQTT Typed Client
//!
//! Demonstrates how to configure MQTT client parameters:
//! - Connection settings (broker, credentials, keep-alive, clean session)
//! - Client behavior (event loop capacity, timeouts, cache sizes)
//! - Publisher settings (QoS levels, message retention)
//! - Subscriber settings (QoS levels, topic caching)

mod shared;

use std::time::Duration;

use mqtt_typed_client::{MqttClient, MqttClientConfig, QoS, WincodeSerializer};
use mqtt_typed_client_macros::mqtt_topic;
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

#[derive(Serialize, Deserialize, SchemaWrite, SchemaRead, Debug)]
struct TemperatureReading {
    temperature: f32,
}

#[derive(Debug)]
#[mqtt_topic("sensors/temperature/{location}/{sensor_id}")]
pub struct TemperatureTopic {
    location: String,
    sensor_id: String,
    payload: TemperatureReading,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    shared::tracing::setup(None);

    println!("Starting MQTT Configuration Example...\n");

    let (host, port) = shared::config::get_mqtt_broker_host_port();
    let client_id = shared::config::get_client_id("temp_sensor");

    println!("Configuring MQTT client for {host}:{port}");

    let mut config = MqttClientConfig::<WincodeSerializer>::new(&client_id, &host, port);

    config
        .connection
        .set_keep_alive(30)
        .set_clean_session(false);

    config.settings.event_loop_capacity = 50;
    config.settings.connection_timeout_millis = 8000;
    config.settings.topic_cache_size = 50;

    let (client, connection) = MqttClient::connect_with_config(config)
        .await
        .inspect_err(|e| {
            shared::config::print_connection_error(&format!("{host}:{port}"), e);
        })?;
    println!("Connected with custom configuration\n");

    let topic_client = client.temperature_topic();
    let mut subscriber = topic_client
        .subscription()
        .with_qos(QoS::AtLeastOnce)
        .with_cache(10)
        .subscribe()
        .await?;

    println!("Subscribed to temperature sensors with cache\n");

    let publisher = topic_client
        .get_publisher("kitchen", "sensor_001")?
        .with_qos(QoS::AtLeastOnce)
        .with_retain(true);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let reading = TemperatureReading { temperature: 22.5 };
    publisher.publish(&reading).await?;
    println!("Published temperature reading with retention\n");

    if let Some(Ok(temp_data)) = subscriber.receive().await {
        println!("=== Received Configuration Demo ===");
        println!("  Location: {}", temp_data.location);
        println!("  Sensor ID: {}", temp_data.sensor_id);
        println!("  Temperature: {}C", temp_data.payload.temperature);
    }

    connection.shutdown().await?;
    println!("\nGoodbye!");
    Ok(())
}
