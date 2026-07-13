//! # 🌟 Quick Start - Recommended API
//!
//! This example demonstrates the **recommended way** to use mqtt-typed-client
//! with ergonomic macros for type-safe MQTT communication.
//!
//! ## What you'll learn:
//! - Type-safe message publishing and subscribing
//! - Automatic topic parameter extraction with `#[mqtt_topic]`
//! - Clean, ergonomic API that prevents runtime errors
//!
//! ## Run this example:
//! ```bash
//! cargo run --example 010_basic_usage
//! ```

use wincode::{SchemaRead, SchemaWrite};
use mqtt_typed_client::{WincodeSerializer, MqttClient};
use mqtt_typed_client_macros::mqtt_topic;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

// === Define your data types ===

#[derive(Serialize, Deserialize, Debug, SchemaWrite, SchemaRead, PartialEq)]
struct SensorReading {
	temperature: f64,
	humidity: f64,
	timestamp: u64,
}

// === Magic happens here: type-safe MQTT topics ===

/// Temperature sensor messages with automatic topic parsing
#[derive(Debug)]
#[mqtt_topic("sensors/{building}/{room}/temperature")]
struct TemperatureSensor {
	// These fields are automatically extracted from the topic:
	building: String, // From "sensors/{building}/..."
	room: String,     // From "sensors/.../room}/..."

	// The actual message data:
	payload: SensorReading,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	// Setup logging
	tracing_subscriber::fmt()
		.with_max_level(tracing::Level::INFO)
		.compact()
		.init();

	info!("🚀 Starting MQTT Typed Client - Basic Usage Example");

	// === 1. Connect to MQTT broker ===
	let (client, connection) = MqttClient::<WincodeSerializer>::connect(
		"mqtt://broker.mqtt.cool:1883?client_id=some_client_1",
	)
	.await?;

	info!("✅ Connected to MQTT broker");

	// Show what patterns are generated
	info!("📋 Topic pattern: {}", TemperatureSensor::TOPIC_PATTERN);
	info!("📋 MQTT pattern:  {}", TemperatureSensor::MQTT_PATTERN);

	// === 2. Set up subscriber (receives messages) ===
	let mut subscriber = TemperatureSensor::subscribe(&client).await?;
	info!("🎧 Subscribed to temperature sensors");

	// === 3. Spawn publisher task (sends messages) ===
	tokio::spawn(async move {
		info!("📡 Starting to publish sensor data...");

		// Simulate different sensors in different locations
		let locations = [
			("OfficeBuilding", "Conference"),
			("OfficeBuilding", "Kitchen"),
			("Warehouse", "Zone1"),
		];

		for (i, (building, room)) in
			locations.iter().cycle().enumerate().take(6)
		{
			let reading = SensorReading {
				temperature: 20.0 + (i as f64 * 2.5),
				humidity: 45.0 + (i as f64 * 3.0),
				timestamp: std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_secs(),
			};

			// 🎯 Type-safe publishing - no string concatenation!
			match TemperatureSensor::publish(&client, building, room, &reading)
				.await
			{
				| Ok(()) => {
					info!(
						building = building,
						room = room,
						temp = reading.temperature,
						"📤 Published sensor reading"
					);
				}
				| Err(e) => error!("❌ Failed to publish: {e}"),
			}
		}

		info!("📡 Publishing completed");
	});

	// === 4. Receive and process messages ===
	let mut message_count = 0;
	info!("🎧 Listening for sensor readings...");

	while let Some(result) = subscriber.receive().await {
		match result {
			| Ok(sensor) => {
				info!(
					building = %sensor.building,
					room = %sensor.room,
					temperature = sensor.payload.temperature,
					humidity = sensor.payload.humidity,
					"🌡️ Received: {}/{}  {}°C  {}%",
					sensor.building,
					sensor.room,
					sensor.payload.temperature,
					sensor.payload.humidity
				);

				message_count += 1;
				if message_count >= 4 {
					info!("✅ Received enough messages, shutting down...");
					break;
				}
			}
			| Err(e) => {
				error!("❌ Failed to parse message: {e}");
			}
		}
	}

	// === 5. Clean shutdown ===
	connection.shutdown().await?;
	info!("🏁 Example completed successfully!");

	println!("\n🎉 Success! You've seen the power of typed MQTT:");
	println!("   ✅ No manual topic string building");
	println!("   ✅ Automatic parameter extraction");
	println!("   ✅ Compile-time type safety");
	println!("   ✅ Runtime error prevention");
	println!(
		"\n💡 Next: Try examples/020_iot_device.rs for real IoT scenarios!"
	);

	Ok(())
}
