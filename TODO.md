# TODO: switch back to crates.io rumqttc-v4-next

This local fork currently points at the repository checkout:

```toml
rumqttc = { package = "rumqttc-v4-next", path = "../rumqtt/rumqttc-v4", default-features = false }
```

Once the crates.io `rumqttc-v4-next` release has the same API shape as this
repository's current main branch, switch the dependency to the published crate:

```toml
rumqttc = { package = "rumqttc-v4-next", version = "0.33.2", default-features = false }
```

Keep the wrapper-side compatibility patches for builder-based client
construction, the `PublishOptions` publish API, and byte-backed incoming publish
topics unless upstream `mqtt-typed-client` has adopted the same API.
