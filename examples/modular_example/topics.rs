#![allow(clippy::mem_forget)]

use std::sync::Arc;

use mqtt_typed_client::topic::topic_match::TopicMatch;
use mqtt_typed_client_macros::mqtt_topic;
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

#[allow(clippy::mem_forget)]
#[derive(Debug, Clone, Serialize, Deserialize, SchemaWrite, SchemaRead)]
pub struct TemperatureReading {
    pub device_id: usize,
    pub temperature: f32,
    pub humidity: Option<f32>,
    pub battery_level: Option<u8>,
}

#[derive(Debug)]
#[mqtt_topic("sensors/{location}/{sensor_type}/{device_id}/data")]
pub struct TemperatureTopic {
    pub location: String,
    pub sensor_type: String,
    pub device_id: usize,
    pub payload: TemperatureReading,
    pub topic: Arc<TopicMatch>,
}
