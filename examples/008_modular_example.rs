//! # Modular MQTT Client Example
//!
//! Demonstrates how to structure a multi-module MQTT application
//! with typed topics and organized imports.

mod modular_example;
mod shared;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    shared::tracing::setup(None);

    println!("Starting Modular MQTT Client example...\n");

    modular_example::run_example().await?;

    println!("\n- Example completed successfully!");
    Ok(())
}
