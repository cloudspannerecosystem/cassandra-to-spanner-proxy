# Google Cloud Spanner Proxy Adaptor - Frequently Asked Questions

### How are CQL Data Types mapped with Cloud Spanner?

    | CQL Type              | Supported | Cloud Spanner Mapping   |
    |-----------------------|:---------:|:-----------------------:|
    | text                  |     ✓     |          STRING         |
    | blob                  |     ✓     |          BYTES          |
    | timestamp             |     ✓     |          TIMESTAMP      |
    | int                   |     ✓     |          INT64          |
    | bigint                |     ✓     |          INT64          |
    | float                 |     ✓     |          FLOAT64        |
    | boolean               |     ✓     |          BOOL           |
    | uuid                  |     ✓     |          STRING         |
    | map<text, boolean>    |     ✓     |          JSON           |
    | map<text, text>       |     ✓     |          JSON           |
    | map<text, timestamp>  |     ✓     |          JSON           |
    | list<text>            |     ✓     |          ARRAY<STRING>  |
    | set<text>             |     ✓     |          ARRAY<STRING>  |

### How are BATCH statements handled in proxy?

Each batch translates to one read-write transaction for atomicity in Spanner.

### How Prepared statements handled in proxy?
In the proxy, we translate the prepared statement (with parameters) and cache the translated spanner sql. Then we return a handle to the client. When the client executes the prepared statement with parameter substitution, we just look up the handle and issue the request against spanner with the parameter substitutions.


### How can I get insights into execution times in Proxy Adaptor?
Proxy Adaptor supports [using OpenTelemetry](../otel/README.md) to collect and export traces and metrics.
You can enable the tracing or the metrics using the configuration file when starting the proxy adapter.
Tracing and metrics can be enabled at the same time or separately.

## What latency does Proxy Adaptor add?

>> TODO

### Contributions 
We are currently not accepting external code contributions to this project.
