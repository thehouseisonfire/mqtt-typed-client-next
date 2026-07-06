//! # All Serializers Demo
//!
//! Demonstrates that all available serializers can be used with the MQTT client.
//! Requires all serializer features to be enabled.

use mqtt_typed_client::{
    CborSerializer, FlexbuffersSerializer, JsonSerializer, MessagePackSerializer,
    MessageSerializer, MqttClient, PostcardSerializer, ProtobufSerializer, RonSerializer,
    WincodeSerializer,
};
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

#[derive(Serialize, Deserialize, SchemaWrite, SchemaRead, Debug, Clone)]
struct TestMessage {
    text: String,
    id: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing all available serializers...\n");

    test_full_cycle::<WincodeSerializer>("Wincode").await?;
    test_full_cycle::<JsonSerializer>("JSON").await?;
    test_full_cycle::<MessagePackSerializer>("MessagePack").await?;
    test_full_cycle::<CborSerializer>("CBOR").await?;
    test_full_cycle::<PostcardSerializer>("Postcard").await?;
    test_full_cycle::<RonSerializer>("RON").await?;
    test_full_cycle::<FlexbuffersSerializer>("Flexbuffers").await?;

    test_connection_only::<ProtobufSerializer>("Protobuf").await?;

    println!("\nAll serializers tested successfully!");
    Ok(())
}

async fn test_full_cycle<S>(name: &str) -> Result<(), Box<dyn std::error::Error>>
where
    S: MessageSerializer<TestMessage> + Default + 'static,
    <S as MessageSerializer<TestMessage>>::SerializeError: std::fmt::Debug,
    <S as MessageSerializer<TestMessage>>::DeserializeError: std::fmt::Debug,
{
    println!("Testing {name} serializer...");

    let url = format!(
        "mqtt://localhost:1883?client_id=test_{}",
        name.to_lowercase()
    );
    let (client, connection) = MqttClient::<S>::connect(&url).await?;

    let topic = format!("test/{}", name.to_lowercase());
    let publisher = client.get_publisher::<TestMessage>(&topic)?;
    let mut subscriber = client.subscribe::<TestMessage>(topic.as_str()).await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let message = TestMessage {
        text: format!("Hello from {name} serializer!"),
        id: 42,
    };

    publisher.publish(&message).await?;

    println!("   Waiting for message...");
    if let Some((topic_match, result)) = subscriber.receive().await {
        match result {
            Ok(received_message) => {
                println!(
                    "   Received from {}: {} (id: {})",
                    topic_match.topic_path(),
                    received_message.text,
                    received_message.id
                );
                println!("{name} (serialize + deserialize successful)");
            }
            Err(e) => {
                println!("{name} (deserialization error: {e:?})");
                return Err(format!("Deserialization failed: {e:?}").into());
            }
        }
    } else {
        println!("{name} (no message received)");
        return Err("No message received".into());
    }

    connection.shutdown().await?;
    Ok(())
}

async fn test_connection_only<S>(name: &str) -> Result<(), Box<dyn std::error::Error>>
where
    S: Default + Clone + Send + Sync + 'static,
{
    println!("Testing {name} serializer (connection only)...");

    let url = format!(
        "mqtt://localhost:1883?client_id=test_{}",
        name.to_lowercase().replace(' ', "_")
    );
    let (client, connection) = MqttClient::<S>::connect(&url).await?;

    let _ = client;

    connection.shutdown().await?;

    println!("{name} (connection successful, messaging requires generated types)");
    Ok(())
}
