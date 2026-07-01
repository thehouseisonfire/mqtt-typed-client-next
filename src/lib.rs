//! # MQTT Typed Client
//!
//! Local fork of `mqtt-typed-client` patched for the protocol-scoped
//! `rumqttc-v4-next` and `rumqttc-v5-next` crates.

#[cfg(all(feature = "rumqttc-v4", feature = "rumqttc-v5"))]
compile_error!("features `rumqttc-v4` and `rumqttc-v5` are mutually exclusive");
#[cfg(not(any(feature = "rumqttc-v4", feature = "rumqttc-v5")))]
compile_error!("enable exactly one of `rumqttc-v4` or `rumqttc-v5`");

#[cfg(feature = "rumqttc-v4")]
extern crate rumqttc_v4 as rumqttc;
#[cfg(all(feature = "rumqttc-v5", not(feature = "rumqttc-v4")))]
extern crate rumqttc_v5 as rumqttc;

pub use mqtt_typed_client_core::*;
#[cfg(feature = "macros")]
pub use mqtt_typed_client_macros::*;

#[cfg(any(
    feature = "rumqttc-use-rustls",
    feature = "rumqttc-use-rustls-no-provider"
))]
pub use rumqttc::tokio_rustls;
#[cfg(any(
    feature = "rumqttc-use-rustls",
    feature = "rumqttc-use-rustls-no-provider"
))]
pub use rumqttc::tokio_rustls::rustls;

pub mod prelude {
    //! Convenient imports for common use cases.

    pub use mqtt_typed_client_core::structured::*;
    #[cfg(feature = "wincode")]
    pub use mqtt_typed_client_core::WincodeSerializer;
    pub use mqtt_typed_client_core::{
        ClientSettings, MessageSerializer, MqttClient, MqttClientConfig, MqttClientError,
        MqttConnection, MqttOptions, MqttPublisher, MqttSubscriber, QoS, Result,
        SubscriptionBuilder, Transport, TypedLastWill,
    };
    #[cfg(feature = "macros")]
    pub use mqtt_typed_client_macros::*;
}

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod info {
    //! Library metadata and version information.

    pub const NAME: &str = env!("CARGO_PKG_NAME");
    pub const VERSION: &str = env!("CARGO_PKG_VERSION");
    pub const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");
}
