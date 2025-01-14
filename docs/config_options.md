# Config Options

```
cassandra_to_spanner_configs:
    # [Optional] The project ID where the Spanner database(s) is/are running.
    # At each listener level, you can specify this, or you can define it here if it is the same for all listeners.
    projectId: YOUR_PROJECT_ID

    # [Optional] Global default configuration table name
    configTableName: YOUR_CONFIG_TABLE_NAME

    # If True, proxy will flatten out {keyspace}_{table_name} as the
    # table name. Otherwise, original table name will be preserved
    # as-is. Default to False
    keyspaceFlatter: True

    # useRowTTL: If set to True, the "spanner_ttl_ts" column will be included in all queries
    # (insert, select, update). If set to False, the "spanner_ttl_ts" column will be ignored
    # and excluded from all query types.Default is False
    useRowTTL: True

    # useRowTimestamp: If set to True, the "last_commit_ts" column will be included in
    # all queries (insert, select, update). If set to False, the "last_commit_ts" column
    # will be excluded from all query types.Default is False
    useRowTimestamp: True

listeners:
  - name: YOUR_CLUSTER_NAME_1

    # The inbound port for the proxy. Defaults to 9042.
    port: PORT_1

    spanner:
        # [Optional], skip if it is the same as global_projectId.
        projectId: YOUR_PROJECT_ID

        # The instance ID where the Spanner database(s) is/are running.
        instanceId: YOUR_SPANNER_INSTANCE_ID

        # The  Spanner database ID to connect to.
        databaseId: YOUR_SPANNER_DATABASE

        # [Optional] - Global else default to TableConfigurations
        configTableName: TableConfigurations

        Session:
            # Minimum number of sessions that Spanner pool will always maintain.
            # Defaults to 100.
            min: 100
            # Maximum number of sessions that Spanner pool will have.
            # Defaults to 400.
            max: 400
            # Number of channels utilized by the Spanner client.
            # Defaults to 4.
            grpcChannels: 4
            # Stale read seconds by default it will be 0
            staleRead: 0

        # Spanner read/write operation settings
        Operation:
          # This is the amount of latency this request is willing to incur in order
          # to improve throughput. If this field is not set, Spanner assumes requests
          # are relatively latency sensitive and automatically determines an appropriate
          # delay time. You can specify a commit delay value between 0 and 500 ms.
          maxCommitDelay: 100

          # With this option, Apply may attempt to apply mutations (Insert/Delete/Update)
          # more than once; if the mutations are not idempotent, this may lead to a
          # failure being reported when the mutation was applied more than once.
          # When enabled, replay protection may require an additional RPC.
          # So this option may be appropriate for latency sensitive and/or high throughput blind writing.
          replayProtection: False

otel:
    # Set enabled to true or false for OTEL metrics and traces
    enabled: True

    # Whether or not to enable client side metrics (such as sessions, gfe latency etc.)
    enabledClientSideMetrics: False

    # Name of the collector service to be setup as a sidecar
    serviceName: YOUR_OTEL_COLLECTOR_SERVICE_NAME

    healthcheck:
        # Enable the health check in this proxy application config only if the
        # "health_check" extension is added to the OTEL collector service configuration.
        #
        # Recommendation:
        # Enable the OTEL health check if you need to verify the collector's availability
        # at the start of the application. For development or testing environments, it can
        # be safely disabled to reduce complexity.
        # Enable/Disable Health Check for OTEL, Default 'False'.
        enabled: False
        # Health check endpoint for the OTEL collector service
        endpoint: YOUR_OTEL_COLLECTOR_HEALTHCHECK_ENDPOINT
    metrics:
        # Collector service endpoint
        endpoint: YOUR_OTEL_COLLECTOR_SERVICE_ENDPOINT
    traces:
        # Collector service endpoint
        endpoint: YOUR_OTEL_COLLECTOR_SERVICE_ENDPOINT
        #Sampling ratio should be between 0 and 1. Here 0.05 means 5/100 Sampling ratio.
        samplingRatio: YOUR_SAMPLING_RATIO

loggerConfig:
    # Specifies the type of output, here it is set to 'file' indicating logs will be written to a file.
    # Value of `outputType` should be `file` for file type or `stdout` for standard output.
    # Default value is `stdout`.
    outputType: YOUR_LOG_OUTPUT_TYPE

    # Set this only if the outputType is set to `file`.
    # The path and name of the log file where logs will be stored. For example, output.log, Required Key.
    # Default `/var/log/cassandra-to-spanner-proxy/output.log`.
    fileName: YOUR_LOG_OUTPUT_PATH

    # Set this only if the outputType is set to `file`.
    # The maximum size of the log file in megabytes before it is rotated. For example, 500 for 500 MB. Default 100MB
    maxSize: MAX_LOG_FILE_SIZE

    # Set this only if the outputType is set to `file`.
    # The maximum number of backup log files to keep. Once this limit is reached, the oldest log file will be deleted. Default '10'.
    maxBackups: MAX_LOG_FILE_BACKUPS

    # Set this only if the outputType is set to `file`.
    # The maximum age in days for a log file to be retained. Logs older than this will be deleted. Required Key.
    # Default 3 days
    maxAge: MAX_LOG_FILE_AGE

    # Set this only if the outputType is set to `file`.
    # Default value is set to 'False'. Change the value to 'True' for compressing the log files.
    compress: True


    # Encoding sets the logger's encoding. Valid values are "json", "key-value" and
	  # "console"
    encoding: ENCODING_TYPE
```

# Example config.yaml

```
cassandra_to_spanner_configs:
  projectId: cassandra-to-spanner
  configTableName: TableConfigurations
  keyspaceFlatter: True

listeners:
  - name: cluster1
    port: 9042
    spanner:
      instanceId: spanner-instance-dev
      databaseId: cluster1

otel:
  enabled: True
  enabledClientSideMetrics: True
  serviceName: cassandra-to-spanner-otel-service
  healthcheck:
    enabled: True
    endpoint: localhost:13133
  metrics:
    endpoint: localhost:4317

  traces:
    endpoint: localhost:4317
    samplingRatio: 0.05

loggerConfig:
  outputType: file
  fileName: output/output.log
  maxSize: 10
  maxBackups: 2
  maxAge: 1
  compress: True
```
