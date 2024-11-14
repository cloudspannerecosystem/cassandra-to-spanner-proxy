# How to Setup & Run proxy against Cloud Spanner Emulator

## Start a local running Emulator database

1. Export environment variable for local Emulator host:
    ```
    export SPANNER_EMULATOR_HOST=localhost:9010
    ```

2. Verify both 9010 (gRPC server) and 9020 (REST server) are not in used:
    ```
    sudo fuser -k 9010/tcp
    sudo fuser -k 9020/tcp
    ```
3. Start a local running Emulator via docker:
    ```
    docker pull gcr.io/cloud-spanner-emulator/emulator
    docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
    ```
    For other ways to start running Emulator, see [Quickstart](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator?tab=readme-ov-file#quickstart) section from [Cloud Spanner Emulator](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator) repo.

4. Activate gcloud configuration for Emulator. If not done already, create a gcloud configuration for easier future usage:
    ```
    gcloud config configurations create emulator
    gcloud config set auth/disable_credentials true
    gcloud config set account emulator-account
    gcloud config set project emulator-project
    gcloud config set api_endpoint_overrides/spanner http://localhost:9020/ 
    gcloud config set instance test-instance
    ```
5. Create a test database to be use:
    ```
    gcloud spanner instances create test-instance --config=regional-us-central1 \
    --description="Test Instance" --nodes=1
    gcloud spanner databases create test-db \
    --instance=test-instance
    ```
6. Populate a sample table and `TableConfigurations` from a CQL schema:
    Create a sample ddcql schemal file `sample_schema.cql` with above schema
    ```
    CREATE TABLE IF NOT EXISTS keyspace1.event (
      user_id TEXT PRIMARY KEY,
      event_data TEXT
    );
    ```
    Use [schema converter](https://github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tree/main/schema_converter) to automatically create mapping table and `TableConfigurations` with key flatten enabled in your emulator database:
    ```
    go run cql_to_spanner_schema_converter.go --project emulator-project --instance test-instance --database test-db --cql /your_path/sample_schema.cql --keyspaceFlatter
    ```
## Running Cassandra to Spanner Proxy against Emulator

1. Modify your config.yaml point to the test database in Emulator
    ```
    cassandra_to_spanner_configs:
      projectId: emulator-project
      configTableName: TableConfigurations
      keyspaceFlatter: True

    listeners:
    - name: test-db
      port: 9042
      spanner:
        instanceId: test-instance
        databaseId: test-db

    ```
2. Follow [Getting started](https://github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tree/main?tab=readme-ov-file#getting-started) to start a local running proxy.

## Start executing CQL queries against Emulator

1. Now you can start issueing CQL queries toward `test-db` in the way you prefer. Use CQLSH as an example:
    ```
    cqlsh
    Connected to cql-proxy at 127.0.0.1:9042
    [cqlsh 6.2.0 | Cassandra 4.0.0.6816 | CQL spec 3.4.5 | Native protocol v4]
    Use HELP for help.
    cqlsh> SELECT * FROM keyspace1.event;

    event_data | user_id
    ------------+---------

    (0 rows)
    cqlsh> INSERT INTO keyspace1.event (user_id, event_data) VALUES ('id1','test_data1');
    cqlsh> SELECT * FROM keyspace1.event;
    user_id | event_data
    ---------+------------
        id1 | test_data1

    (1 rows)
    ```

2. Verify that the row was successfully inserted into Emulator database by querying with gcloud directly:
    ```
    gcloud spanner databases execute-sql test-db  --instance=test-instance --sql='SELECT * FROM keyspace1_event'
    user_id  event_data
    id1      test_data1
    ```
