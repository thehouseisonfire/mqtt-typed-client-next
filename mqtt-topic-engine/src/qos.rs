//! Quality of Service (`QoS`) levels for MQTT
//!
//! Defines the three standard MQTT `QoS` levels independent of any specific
//! MQTT client implementation.
//!
//! This module provides conversions to/from popular MQTT client libraries:
//! - `rumqttc` - Enable with feature `rumqttc`
//! - `paho-mqtt` - Enable with feature `paho-mqtt`
//! - `ntex-mqtt` - Enable with feature `ntex-mqtt`
//!
//! These features only add **type conversions** between [`QoS`] and the client's
//! own `QoS` type — they pull in the client crate purely for its types and do not
//! drive any connection. How that client itself is built stays under your
//! control: this crate depends on each with `default-features = false`, so it
//! never forces a native toolchain on you. In particular `paho-mqtt` links a
//! C library — its default `bundled` feature builds it from source (needs `CMake`)
//! while otherwise it expects a system-installed Paho C library. Since you would
//! only enable the `paho-mqtt` feature when you already use `paho-mqtt` as your
//! client, Cargo's (additive) feature unification applies your own build choice
//! there, and everything links as expected.

use std::fmt;

/// MQTT Quality of Service levels
///
/// Defines delivery guarantees for MQTT messages:
/// - `AtMostOnce` (0): Best effort delivery, no guarantees
/// - `AtLeastOnce` (1): Message delivered at least once, duplicates possible
/// - `ExactlyOnce` (2): Message delivered exactly once, highest guarantee
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum QoS {
    /// `QoS` 0: At most once delivery (fire and forget)
    AtMostOnce = 0,
    /// `QoS` 1: At least once delivery (acknowledged delivery)
    AtLeastOnce = 1,
    /// `QoS` 2: Exactly once delivery (assured delivery)
    ExactlyOnce = 2,
}

impl QoS {
    /// Convert to rumqttc `QoS` type
    ///
    /// # Example
    /// ```ignore
    /// let qos = QoS::AtLeastOnce;
    /// let rumqttc_qos = qos.to_rumqttc();
    /// ```
    #[cfg(any(feature = "rumqttc", feature = "rumqttc-v4", feature = "rumqttc-v5"))]
    #[must_use]
    pub const fn to_rumqttc(self) -> crate::rumqttc::QoS {
        match self {
            Self::AtMostOnce => crate::rumqttc::QoS::AtMostOnce,
            Self::AtLeastOnce => crate::rumqttc::QoS::AtLeastOnce,
            Self::ExactlyOnce => crate::rumqttc::QoS::ExactlyOnce,
        }
    }

    /// Convert to paho-mqtt `QoS` type
    ///
    /// # Example
    /// ```ignore
    /// let qos = QoS::AtLeastOnce;
    /// let paho_qos = qos.to_paho_mqtt();
    /// ```
    #[cfg(feature = "paho-mqtt")]
    #[must_use]
    pub const fn to_paho_mqtt(self) -> paho_mqtt::QoS {
        match self {
            Self::AtMostOnce => paho_mqtt::QoS::AtMostOnce,
            Self::AtLeastOnce => paho_mqtt::QoS::AtLeastOnce,
            Self::ExactlyOnce => paho_mqtt::QoS::ExactlyOnce,
        }
    }

    /// Convert to ntex-mqtt `QoS` type
    ///
    /// # Example
    /// ```ignore
    /// let qos = QoS::AtLeastOnce;
    /// let ntex_qos = qos.to_ntex_mqtt();
    /// ```
    #[cfg(feature = "ntex-mqtt")]
    #[must_use]
    pub const fn to_ntex_mqtt(self) -> ntex_mqtt::QoS {
        match self {
            Self::AtMostOnce => ntex_mqtt::QoS::AtMostOnce,
            Self::AtLeastOnce => ntex_mqtt::QoS::AtLeastOnce,
            Self::ExactlyOnce => ntex_mqtt::QoS::ExactlyOnce,
        }
    }
}

#[cfg(any(feature = "rumqttc", feature = "rumqttc-v4", feature = "rumqttc-v5"))]
impl From<crate::rumqttc::QoS> for QoS {
    fn from(qos: crate::rumqttc::QoS) -> Self {
        match qos {
            crate::rumqttc::QoS::AtMostOnce => Self::AtMostOnce,
            crate::rumqttc::QoS::AtLeastOnce => Self::AtLeastOnce,
            crate::rumqttc::QoS::ExactlyOnce => Self::ExactlyOnce,
        }
    }
}

#[cfg(feature = "paho-mqtt")]
impl From<paho_mqtt::QoS> for QoS {
    fn from(qos: paho_mqtt::QoS) -> Self {
        match qos {
            paho_mqtt::QoS::AtMostOnce => Self::AtMostOnce,
            paho_mqtt::QoS::AtLeastOnce => Self::AtLeastOnce,
            paho_mqtt::QoS::ExactlyOnce => Self::ExactlyOnce,
        }
    }
}

#[cfg(feature = "ntex-mqtt")]
impl From<ntex_mqtt::QoS> for QoS {
    fn from(qos: ntex_mqtt::QoS) -> Self {
        match qos {
            ntex_mqtt::QoS::AtMostOnce => Self::AtMostOnce,
            ntex_mqtt::QoS::AtLeastOnce => Self::AtLeastOnce,
            ntex_mqtt::QoS::ExactlyOnce => Self::ExactlyOnce,
        }
    }
}

impl fmt::Display for QoS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AtMostOnce => write!(f, "QoS0"),
            Self::AtLeastOnce => write!(f, "QoS1"),
            Self::ExactlyOnce => write!(f, "QoS2"),
        }
    }
}
