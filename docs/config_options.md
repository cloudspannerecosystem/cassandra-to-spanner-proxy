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

    # Name of the collector service to be setup as a sidecar
    serviceName: YOUR_OTEL_COLLECTOR_SERVICE_NAME
    
    healthcheck:
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
  serviceName: cassandra-to-spanner-otel-service
  healthcheck:
    endpoint: localhost:13133
  metrics:
    endpoint: localhost:4317

  traces:
    endpoint: localhost:4317
    samplingRatio: 0.05
```