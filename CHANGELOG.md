# Changelog


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