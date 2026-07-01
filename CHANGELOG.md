# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Fork crate to be compatible with `rumqttc-next` v4 and v5
- Switch default serializer from `bincode` to `wincode`
- Port examples from upstream, adapted for `WincodeSerializer` and `rumqttc-v4`
- Bump `lru` from 0.15 to 0.18
- Bump `prost` from 0.12 to 0.14 (MSRV 1.85)
- Bump `ron` from 0.8 to 0.12 (MSRV 1.64)
- Bump `flexbuffers` from 2.0 to 25.12

### Fixed

- Fix `ProtobufSerializer` error types: replace non-existent `prost::SchemaWriteError`/`prost::SchemaReadError` with correct `prost::EncodeError`/`prost::DecodeError`
