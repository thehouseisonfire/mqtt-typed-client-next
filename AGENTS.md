# Repository Guidelines

## Project Structure & Module Organization

This is a Rust 2024 workspace with three member crates plus a root re-export crate.
The root crate lives in `src/lib.rs` and exposes the public `mqtt_typed_client`
API. Core client logic is in `core/src/`, procedural macro parsing and code
generation are in `macros/src/`, and MQTT topic matching/routing is in
`mqtt-topic-engine/src/`. Runnable examples are in `examples/`, with shared
example helpers under `examples/shared/` and modular example files under
`examples/modular_example/`. Tests are colocated with crate sources, commonly as
`*_tests.rs` modules.

## Build, Test, and Development Commands

- `cargo check --workspace` verifies all workspace crates compile.
- `cargo test --workspace` runs unit tests for the core, macro, and topic engine
  crates.
- `cargo fmt --all --check` checks Rust formatting without modifying files.
- `cargo clippy --workspace --all-targets -- -D warnings` runs lint checks with
  warnings treated as errors.
- `cargo run --example 000_hello_world` runs a basic example; replace the example
  name with another file from `examples/`.

The workspace depends on local `rumqttc-v4-next` and `rumqttc-v5-next` paths under
`../rumqtt/`, so ensure those sibling crates exist before running full builds.

## Coding Style & Naming Conventions

Use standard `rustfmt` formatting for Rust code with 4-space indentation. Keep
module and file names in `snake_case`; public types and traits use `PascalCase`;
functions, methods, variables, and feature names use `snake_case` or existing
Cargo feature naming. Prefer small modules that match the current layout, such as
`client/config.rs` or `routing/subscriber.rs`. Keep feature gates explicit because
`rumqttc-v4` and `rumqttc-v5` backends are mutually exclusive.

## Testing Guidelines

Add focused unit tests near the code they exercise, following the existing
`*_tests.rs` pattern in `macros/src/` and `mqtt-topic-engine/src/`. For macro
changes, cover both analysis and generated-code behavior where practical. For
topic matching changes, include wildcard, parameter extraction, and invalid-topic
cases. Run `cargo test --workspace` before opening a PR.

## Commit & Pull Request Guidelines

Recent commits use short, imperative, lowercase summaries such as `lint`,
`clippy`, and `bump deps`; keep commit subjects concise and scoped. Pull requests
should describe the behavior change, mention affected crates or feature flags,
link related issues when applicable, and include test results such as
`cargo test --workspace` and `cargo clippy --workspace --all-targets -- -D warnings`.
For example or API changes, update `README.md`, crate READMEs, or relevant
examples in the same PR.
