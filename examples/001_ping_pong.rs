//! # Ping Pong Game - MQTT Typed Client Example
//!
//! Demonstrates interactive bi-directional communication between two players
//! through MQTT topics using the typed client pattern.
//!
//! Usage:
//! ```bash
//! # Terminal 1: Start as subscriber (Alice)
//! cargo run --example 001_ping_pong
//!
//! # Terminal 2: Run as publisher (Bob)
//! cargo run --example 001_ping_pong -- --publisher
//! ```

#![allow(clippy::mem_forget)]

mod shared;

use std::time::Duration;

use mqtt_typed_client::{MqttClient, MqttClientError, WincodeSerializer};
use mqtt_typed_client_macros::mqtt_topic;
use rand::{RngExt, rng};
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

#[allow(clippy::mem_forget)]
#[derive(Serialize, Deserialize, SchemaWrite, SchemaRead, Debug)]
enum PingPongMessage {
    Ping(usize),
    Pong(usize),
    GameOver,
}

impl PingPongMessage {
    fn next_move(&self) -> PingPongMessage {
        if rng().random_bool(0.05) {
            return PingPongMessage::GameOver;
        }
        match self {
            PingPongMessage::Ping(n) => PingPongMessage::Pong(n + 1),
            PingPongMessage::Pong(n) => PingPongMessage::Ping(n + 1),
            PingPongMessage::GameOver => PingPongMessage::GameOver,
        }
    }
    fn is_game_over(&self) -> bool {
        matches!(self, PingPongMessage::GameOver)
    }
}

#[mqtt_topic("game/{player}")]
pub struct PingPongTopic {
    payload: PingPongMessage,
}

async fn run_player(
    client: MqttClient<WincodeSerializer>,
    player: &str,
    other_player: &str,
    is_starter: bool,
) -> Result<(), MqttClientError> {
    let topic_client = client.ping_pong_topic();

    let mut subscriber = topic_client
        .subscription()
        .for_player(player)
        .subscribe()
        .await?;

    if is_starter {
        let ping_message = PingPongMessage::Ping(0);
        println!("{player:>10}: starts the game with {ping_message:?}\n");
        topic_client.publish(other_player, &ping_message).await?;
    }

    while let Some(result) = subscriber.receive().await {
        match result {
            Ok(response) => {
                println!("{player:>10} received: {:?}", response.payload);

                if response.payload.is_game_over() {
                    println!("{player:>10} Yarrr! I am the winner!");
                    break;
                }

                let reply = response.payload.next_move();
                topic_client.publish(other_player, &reply).await?;

                if reply.is_game_over() {
                    println!("{player:>10}: Ups... I'm lost...");
                    break;
                }
            }
            Err(err) => {
                eprintln!("{player:>10} deserialization error: {err:?}");
                continue;
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    shared::tracing::setup(None);

    println!("Starting MQTT Ping Pong example...\n");

    let connection_url = shared::config::build_url("ping_pong");
    println!("Connecting to MQTT broker: {connection_url}");

    let (client, connection) = MqttClient::<WincodeSerializer>::connect(&connection_url)
        .await
        .inspect_err(|e| {
            shared::config::print_connection_error(&connection_url, e);
        })?;

    println!("- Connected to MQTT broker\n");

    let client_clone = client.clone();
    let alice_handler = async move { run_player(client_clone, "alice", "bob", false).await };
    let bob_handler = async move {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        run_player(client, "bob", "alice", true).await
    };

    let (alice_result, bob_result) = tokio::join!(alice_handler, bob_handler);
    alice_result?;
    bob_result?;

    connection.shutdown().await?;
    println!("\n- Goodbye!");

    Ok(())
}
