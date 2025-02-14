# Changelog

# [1.0.6] - 2025-02-14
### Bug Fixes
* Return VoidResult for Batch message.

# [1.0.5] - 2025-02-10

### Features
* Support a configurable readiness check endpoint via HTTP.

### Enhancements
* Add label "spanner_api" in request count and latency metrics for granular observability.
* Automatically set project and instances to defeault for external hosts to simplify the connection configuration.

# [1.0.4] - 2025-02-03

### Features
* Allow users to disable the generation of a random service instance ID key for Otel.
* Support external host connections to the Spanner endpoint over plaintext, TLS, and mTLS in the Cassandra proxy adapter.

# [1.0.3] - 2025-01-24

### Features
* Add logging on whether direct path feature is enabled on proxy startup.

### Bug Fixes
* Fix file logger to support KeyValue encoding.

# [1.0.2] - 2025-01-22

### Features
* Add support for USE keyspace
* Add flag to enable/disable metrics and traces separately
* Allow key-value encoding for logging format

### Others
* Remove always on Direct path setting
* Bump dependencies
* Add documentation on running proxy against Emulator


## [1.0.1](https://github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/compare/v1.0.0...v1.0.1)

### Features
* Add CQL to Spanner Table Converter tool
* Support `double` type

### BREAKING CHANGES
* Add support for useRowTTL and useRowTimestamp options, defaults to `False`

### Bug Fixes
* Allow NULL value in nullable column


## [1.0.0](https://github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/releases/tag/v1.0.0)

Initial release