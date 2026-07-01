# MQTT Typed Client Core

Core functionality for type-safe async MQTT client with automatic topic routing.

This crate contains the fundamental components:
- MQTT client implementation with async/await support
- Topic pattern matching and wildcard routing
- Message serialization abstraction
- Connection management and error handling

## Usage

This crate is typically used through the main `mqtt-typed-client` crate which provides a more ergonomic API with procedural macros.

For direct usage:

```rust
use mqtt_typed_client_core::{MqttClient, WincodeSerializer};

let (client, connection) = MqttClient::<WincodeSerializer>::connect("mqtt://broker.example.com").await?;
```

See the main [mqtt-typed-client](https://crates.io/crates/mqtt-typed-client) crate for complete examples and documentation.

## Features

- `wincode-serializer` - Wincode message serialization (default)
- `json` - JSON message serialization (default)
- `messagepack` - MessagePack serialization
- `cbor` - CBOR serialization
- `postcard` - Postcard serialization
- `protobuf` - Protocol Buffers serialization
- `ron` - RON (Rusty Object Notation) serialization
- `flexbuffers` - FlatBuffers FlexBuffers serialization

## License

This project is licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](https://github.com/holovskyi/mqtt-typed-client/blob/main/LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](https://github.com/holovskyi/mqtt-typed-client/blob/main/LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
