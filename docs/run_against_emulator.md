# How to Setup & Run proxy against Cloud Spanner Emulator

## Start a local running Emulator database

0. Export environment variable for local Emulator host:
    ```
    export SPANNER_EMULATOR_HOST=localhost:9010
    ```

1. Verify both 9010 (gRPC server) and 9020 (REST server) are not in used:
    ```
    sudo fuser -k 9010/tcp
    sudo fuser -k 9020/tcp
    ```
2. Start a local running Emulator via docker:
    ```
    docker pull gcr.io/cloud-spanner-emulator/emulator
    docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
    ```
    For other ways to start running Emulator, see [Quickstart](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator?tab=readme-ov-file#quickstart) section from [Cloud Spanner Emulator](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator) repo.

3. If not done already, create a gcloud configuration for easier future usage:
    ```
    gcloud config configurations create emulator
    gcloud config set auth/disable_credentials true
    gcloud config set account emulator-account
    gcloud config set project emulator-project
    gcloud config set api_endpoint_overrides/spanner http://localhost:9020/ 
    gcloud config set instance test-instance
    ```
4. Create a test database to be use:
    ```
    gcloud spanner instances create test-instance --config=regional-us-central1 \
    --description="Test Instance" --nodes=1
    gcloud spanner databases create test-db \
    --instance=test-instance
    ```
5. Populate a sample table and fill TableConfiguration:
    Create a sample ddl file `sample_schema.txt` with above schema
    ```
    CREATE TABLE IF NOT EXISTS keyspace1_event ( 
    user_id STRING(MAX),
    upload_time INT64,
    event_data BYTES(MAX),
    spanner_ttl_ts TIMESTAMP DEFAULT (NULL),
    last_commit_ts TIMESTAMP NOT NULL OPTIONS ( allow_commit_timestamp = TRUE ),
    )
    PRIMARY KEY
    (user_id,
        upload_time),
    ROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts,
        INTERVAL 0 DAY));

    CREATE TABLE IF NOT EXISTS TableConfiguration (
        KeySpaceName STRING(MAX),
        TableName STRING(MAX),
        ColumnName STRING(MAX),
        ColumnType STRING(MAX),
        IsPrimaryKey BOOL,
        PK_Precedence INT64,
    ) PRIMARY KEY (TableName, ColumnName, KeySpaceName);
    ```
    Create a database in Emulator with above sample schema:
    ```
    gcloud spanner databases ddl update test-db --instance=test-instance --ddl-file=sample_schema.txt
    ```
    Fill in `TableConfigurtions`:
    ```
    gcloud spanner databases execute-sql test-db  --instance=test-instance    --sql="INSERT OR UPDATE INTO TableConfiguration (KeySpaceName, TableName, ColumnName, ColumnType, IsPrimaryKey, PK_Precedence) VALUES 
    ('keyspace1', 'event', 'user_id', 'text', true, 1),
    ('keyspace1', 'event', 'upload_time', 'bigint', true, 2),
    ('keyspace1', 'event', 'event_data', 'blob', false, 0);"
    ```
## Running Cassandra to Spanner Proxy against Emulator

1. Modify your config.yaml point to the test database in Emulator
    ```
    cassandra_to_spanner_configs:
      projectId: emulator-project
      configTableName: TableConfiguration
      keyspaceFlatter: True
      useRowTTL: True
      useRowTimestamp: True

    listeners:
    - name: test-db
      port: 9042
      spanner:
        instanceId: test-instance
        databaseId: test-db

    ```
2. Follow [Getting started](https://github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tree/main?tab=readme-ov-file#getting-started) to start a local running proxy.

## Start executeting CQL queries against Emulator

1. Now you can start issueing CQL queries toward `test-db` in the way you prefer. Use CQLSH as an example:
    ```
    cqlsh
    Connected to cql-proxy at 127.0.0.1:9042
    [cqlsh 6.2.0 | Cassandra 4.0.0.6816 | CQL spec 3.4.5 | Native protocol v4]
    Use HELP for help.
    cqlsh> SELECT * FROM keyspace1.event;

    event_data | user_id | upload_time
    ------------+---------+-------------

    (0 rows)
    cqlsh> INSERT INTO keyspace1.event (user_id, event_data) VALUES (20,'test_20');
    cqlsh> SELECT * FROM keyspace1.event;

    user_id | upload_time | event_data
    ---------+-------------+------------------
        20 |        null | 0x746573745f3230

    ```

2. Verify that the row was successfully inserted into Emulator database by querying with gcloud directly:
    ```
    gcloud spanner databases execute-sql test-db  --instance=test-instance --sql='SELECT * FROM keyspace1_event'
    user_id  upload_time  event_data    spanner_ttl_ts  last_commit_ts
    20       None         dGVzdF8yMA==  None            2024-11-06T01:06:54.282959Z
    ```
