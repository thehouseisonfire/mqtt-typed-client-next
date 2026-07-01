# TODO: switch back to crates.io rumqttc-next packages

This local fork currently points at the repository checkout for both supported
rumqttc backends:

```toml
rumqttc-v4 = { package = "rumqttc-v4-next", version = "0.33.0", path = "../rumqtt/rumqttc-v4", default-features = false }
rumqttc-v5 = { package = "rumqttc-v5-next", version = "0.33.0", path = "../rumqtt/rumqttc-v5", default-features = false }
```

Once the crates.io `rumqttc-v4-next` and `rumqttc-v5-next` releases have the
same API shape as this repository's current main branch, remove the path keys
and keep the published package coordinates:

```toml
rumqttc-v4 = { package = "rumqttc-v4-next", version = "<published-version-with-this-api>", default-features = false }
rumqttc-v5 = { package = "rumqttc-v5-next", version = "0.33.0", default-features = false }
```

Keep the wrapper-side compatibility patches for builder-based client
construction, the `PublishOptions` publish API, and byte-backed incoming publish
topics unless upstream `mqtt-typed-client` has adopted the same API. Keep
`rumqttc-v4` and `rumqttc-v5` mutually exclusive features unless a separate
multi-backend abstraction is introduced.

Publishing notes:

- Publish the workspace packages under the `-next` IDs:
  `mqtt-topic-engine-next`, `mqtt-typed-client-core-next`,
  `mqtt-typed-client-macros-next`, and `mqtt-typed-client-next`.
- `cargo package --workspace --allow-dirty --no-verify` currently succeeds.
- Full `cargo package --workspace --allow-dirty` is expected to fail until the
  crates.io `rumqttc-v4-next`/`rumqttc-v5-next` releases expose the same API as
  the local rumqtt checkout. The current registry `rumqttc-v4-next` package
  still lacks the `PublishOptions` publish API used by this fork.
