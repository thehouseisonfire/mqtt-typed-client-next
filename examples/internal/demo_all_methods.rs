//! Complete test demonstrating new builder-based subscription API
//!
//! This shows how to use the new builder pattern with mqtt_topic macro

use wincode::{SchemaRead, SchemaWrite};
use mqtt_typed_client::{WincodeSerializer, MqttClient, QoS};
use mqtt_typed_client_macros::mqtt_topic;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, SchemaWrite, SchemaRead, PartialEq)]
struct SensorReading {
	temperature: f64,
	humidity: f64,
	timestamp: u64,
}

#[allow(dead_code)]
#[mqtt_topic("sensors/{building}/{floor}/temp/{sensor_id}")]
#[derive(Debug)]
struct TemperatureSensor {
	building: String,
	floor: u32,
	sensor_id: String,
	payload: SensorReading,
}

async fn demonstrate_builder_api() -> Result<(), Box<dyn std::error::Error>> {
	println!("🧪 Demonstrating new builder-based subscription API");

	// This would work with a real MQTT broker:
	let (client, connection) = MqttClient::<WincodeSerializer>::connect(
		"mqtt://localhost:1883?client_id=test_builder",
	)
	.await?;

	println!("📋 New subscription methods:");

	// Method 1: Simple subscription (unchanged for backward compatibility)
	println!("1️⃣  Simple: TemperatureSensor::subscribe()");
	let _simple_subscriber = TemperatureSensor::subscribe(&client).await?;
	println!("    ✅ Subscribed to: {}", TemperatureSensor::MQTT_PATTERN);

	// Method 2: Builder with cache
	println!("2️⃣  Builder with cache: subscription().with_cache(1000)");
	let _cached_subscriber = TemperatureSensor::subscription(&client)
		.with_cache(1000)
		.subscribe()
		.await?;
	println!("    ✅ Subscribed with LRU cache (1000 entries)");

	// Method 3: Builder with QoS
	println!("3️⃣  Builder with QoS: subscription().with_qos(ExactlyOnce)");
	let _qos_subscriber = TemperatureSensor::subscription(&client)
		.with_qos(QoS::ExactlyOnce)
		.subscribe()
		.await?;
	println!("    ✅ Subscribed with QoS::ExactlyOnce");

	// Method 4: Builder with full config
	// println!("4️⃣  Builder with config: subscription().with_config()");
	// let config = SubscriptionConfig {
	// 	qos: QoS::ExactlyOnce,
	// };
	// let _config_subscriber = TemperatureSensor::subscription(&client)
	// 	//.with_config(config)
	// 	.subscribe()
	// 	.await?;
	// println!("    ✅ Subscribed with custom SubscriptionConfig");

	// Method 5: Builder with custom pattern
	println!("5️⃣  Builder with custom pattern: subscription().with_pattern()");
	let _pattern_subscriber = TemperatureSensor::subscription(&client)
		.with_pattern("data/{building}/{floor}/temperature/{sensor_id}")?
		.subscribe()
		.await?;
	println!("    ✅ Subscribed to custom pattern: data/+/+/temperature/+");

	// Method 6: Builder chain (everything together)
	println!(
		"6️⃣  Full builder chain: \
		 subscription().with_cache().with_qos().with_pattern()"
	);
	let _full_subscriber = TemperatureSensor::subscription(&client)
		.with_cache(500)
		.with_qos(QoS::ExactlyOnce)
		.with_pattern("iot/{building}/{floor}/temp/{sensor_id}")?
		.subscribe()
		.await?;
	println!("    ✅ Full chain: cache + QoS + custom pattern");

	// Method 7: Reusable builder template
	println!("7️⃣  Reusable builder template");
	let high_performance_template = TemperatureSensor::subscription(&client)
		.with_cache(1000)
		.with_qos(QoS::ExactlyOnce);

	let _building_a_subscriber = high_performance_template
		.clone()
		.with_pattern("building_a/{building}/{floor}/temp/{sensor_id}")?
		.subscribe()
		.await?;

	let _building_b_subscriber = high_performance_template
		.clone()
		.with_pattern("building_b/{building}/{floor}/temp/{sensor_id}")?
		.subscribe()
		.await?;

	println!("    ✅ Template reused for multiple patterns");

	// Method 8: Testing validation (should fail)
	println!("8️⃣  Testing pattern validation (should fail):");
	let invalid_result = match TemperatureSensor::subscription(&client)
		.with_pattern("wrong/{floor}/{building}/temp/{sensor_id}")
	{
		| Ok(builder) => builder.subscribe().await,
		| Err(e) => Err(e),
	};
	match invalid_result {
		| Err(e) => {
			println!("    ✅ Correctly rejected invalid pattern: {e}")
		}
		| Ok(_) => panic!("Should have failed!"),
	}

	connection.shutdown().await?;

	Ok(())
}

async fn demonstrate_generated_methods()
-> Result<(), Box<dyn std::error::Error>> {
	println!("\n🔍 Testing generated methods:");

	// Test constants (unchanged)
	println!("📊 Pattern constants:");
	println!("   TOPIC_PATTERN: {}", TemperatureSensor::TOPIC_PATTERN);
	println!("   MQTT_PATTERN:  {}", TemperatureSensor::MQTT_PATTERN);

	// Test new methods
	println!("🆕 New methods:");
	let default_pattern = TemperatureSensor::default_pattern();
	println!("   default_pattern(): {}", default_pattern.topic_pattern());

	// Test publisher methods (unchanged)
	println!("📤 Publisher methods (unchanged):");
	println!("✅ publish() - available");
	println!("✅ get_publisher() - available");

	Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	demonstrate_builder_api().await?;
	demonstrate_generated_methods().await?;
	println!("\n🎉 New builder API demonstrated successfully!");
	Ok(())
}
