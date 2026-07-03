use std::env;

use uuid::Uuid;

/// Get MQTT broker URL from environment variable or use default
///
/// Loads configuration from .env files in this order:
/// 1. examples/.env.local (if exists, ignored by git)
/// 2. examples/.env (committed defaults)
/// 3. Environment variables
/// 4. Hardcoded default
#[allow(dead_code)]
pub fn broker_url() -> String {
    drop(dotenvy::from_filename("examples/.env"));
    if std::path::Path::new("examples/.env.local").exists() {
        drop(dotenvy::from_filename("examples/.env.local"));
    }

    env::var("MQTT_BROKER").unwrap_or_else(|_| "mqtt://localhost:1883".to_string())
}

/// Generate unique client ID with given prefix
#[allow(dead_code)]
pub fn get_client_id(prefix: &str) -> String {
    let uuid = Uuid::new_v4().to_string();
    let short_uuid: String = uuid.chars().take(8).collect();
    format!("{prefix}_{short_uuid}")
}

/// Build complete MQTT URL with client ID
#[allow(dead_code)]
pub fn build_url(client_id_prefix: &str) -> String {
    let base_url = broker_url();
    let client_id = get_client_id(client_id_prefix);

    if base_url.contains('?') {
        format!("{base_url}&client_id={client_id}")
    } else {
        format!("{base_url}?client_id={client_id}")
    }
}

/// Print helpful connection error message with troubleshooting tips
#[allow(dead_code)]
pub fn print_connection_error(url: &str, error: &dyn std::error::Error) {
    let example_name = get_example_name();

    eprintln!("Connection failed to: {url}");
    eprintln!("   Error: {error}");
    eprintln!();
    eprintln!("Troubleshooting:");

    if url.contains("localhost") {
        eprintln!("   - Start local MQTT broker: cd dev && docker-compose up -d");
        eprintln!("   - Check if broker is running: docker-compose ps");
    } else {
        eprintln!("   - Check network connection");
        eprintln!("   - Verify broker URL is correct");
    }

    eprintln!(
        "   - Try a public broker: \
		 MQTT_BROKER=\"mqtt://broker.hivemq.com:1883\" cargo run --example \
		 {example_name}"
    );
    eprintln!(
        "   - Enable debug logs: RUST_LOG=debug cargo run --example \
		 {example_name}"
    );
}

/// Parse MQTT broker URL to extract host and port
#[allow(dead_code)]
pub fn get_mqtt_broker_host_port() -> (String, u16) {
    let url = broker_url();

    if let Some(without_scheme) = url
        .strip_prefix("mqtt://")
        .or_else(|| url.strip_prefix("mqtts://"))
    {
        let is_tls = url.starts_with("mqtts://");
        let default_port = if is_tls { 8883 } else { 1883 };

        if let Some((host, port_str)) = without_scheme.split_once(':') {
            let port_part = port_str.split('?').next().unwrap_or(port_str);
            if let Ok(port) = port_part.parse::<u16>() {
                return (host.to_string(), port);
            }
        } else {
            let host = without_scheme.split('?').next().unwrap_or(without_scheme);
            return (host.to_string(), default_port);
        }
    }

    ("localhost".to_string(), 1883)
}

/// Extract example name from current executable path
#[allow(dead_code)]
fn get_example_name() -> String {
    std::env::args()
        .next()
        .and_then(|path| {
            std::path::Path::new(&path)
                .file_stem()
                .and_then(|name| name.to_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "example".to_string())
}
