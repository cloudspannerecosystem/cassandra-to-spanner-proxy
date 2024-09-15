# How to Setup & Execute Unit Test Cases on a Local Machine

## Execute Unit Tests

1. Run the unit test cases with code coverage:
    ```sh
    go test ./{/otel,/third_party/datastax/proxycore,/responsehandler,/spanner,/tableConfig,/tableConfig,/translator,/utilities,/third_party/datastax/proxy} -coverprofile=cover.out
    ```

## Change or Update DDL or Table Configuration DML

DDL and DML will be set up in an emulated Spanner instance before the unit tests are executed. You can include new DDL and DML statements to introduce new unit tests using those tables.

1. **Update DDL**: Modify the schema in [DDL](/test_data/schema.sql).

2. **Update DML**: Modify the data manipulation statements in [DML](./test_data/dml.sql).

## Notes
- Ensure any changes to the DDL or DML are committed before running the unit tests.
- The test scripts will automatically apply the updated DDL and DML to the emulated Spanner instance.

# How to Setup & Execute Integration Test Cases on a Local Machine

## Prerequisites

Ensure Docker is running on your local machine.

## Setup DDL and DML in Spanner Instance

### Create Table
```sql
CREATE TABLE IF NOT EXISTS keyspace1_validate_data_types(
    text_col STRING(MAX),
    blob_col BYTES(MAX),
    timestamp_col TIMESTAMP,
    int_col INT64,
    bigint_col INT64,
    float_col FLOAT64,
    bool_col BOOL,
    uuid_col STRING(36),
    map_bool_col JSON,  
    map_str_col JSON,
    map_ts_col JSON,
    list_str_col ARRAY<STRING(MAX)>,
    set_str_col ARRAY<STRING(MAX)>,
    spanner_ttl_ts TIMESTAMP DEFAULT (NULL),
    last_commit_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (uuid_col DESC),
  ROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts, INTERVAL 0 DAY));

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
```

### Insert or Update Table Configurations
```sql
INSERT OR UPDATE INTO TableConfigurations (KeySpaceName, TableName, ColumnName, ColumnType, IsPrimaryKey, PK_Precedence) VALUES 
('keyspace1', 'validate_data_types', 'text_col', 'text', false, 0),
('keyspace1', 'validate_data_types', 'blob_col', 'blob', false, 0),
('keyspace1', 'validate_data_types', 'timestamp_col', 'timestamp', false, 0),
('keyspace1', 'validate_data_types', 'int_col', 'int', false, 0),
('keyspace1', 'validate_data_types', 'bigint_col', 'bigint', false, 0),  
('keyspace1', 'validate_data_types', 'float_col', 'float', false, 0),
('keyspace1', 'validate_data_types', 'bool_col', 'boolean', false, 0),
('keyspace1', 'validate_data_types', 'uuid_col', 'uuid', true, 1),
('keyspace1', 'validate_data_types', 'map_bool_col', 'map<text, boolean>', false, 0),
('keyspace1', 'validate_data_types', 'map_str_col', 'map<text, text>', false, 0),
('keyspace1', 'validate_data_types', 'map_ts_col', 'map<text, timestamp>', false, 0),
('keyspace1', 'validate_data_types', 'list_str_col', 'list<text>', false, 0),
('keyspace1', 'validate_data_types', 'set_str_col', 'set<text>', false, 0);

INSERT OR UPDATE INTO TableConfigurations (KeySpaceName, TableName, ColumnName, ColumnType, IsPrimaryKey, PK_Precedence) VALUES 
('keyspace1', 'event', 'user_id', 'text', true, 1),
('keyspace1', 'event', 'upload_time', 'bigint', true, 2),
('keyspace1', 'event', 'event_data', 'blob', false, 0);
```

## Setup Environment Variables

1. Set up the `GOOGLE_APPLICATION_CREDENTIALS` environment variable with the appropriate project credentials:
    ```sh
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials-file.json"
    ```

2. Set up the Google Cloud credentials JSON in Cloud Secret
    
    [Setup Secret in Google cloud secret manager](https://cloud.google.com/secret-manager/docs/create-secret-quickstart)

3. Set up the secret path as an environment variable `INTEGRATION_TEST_CRED_PATH`:
    ```sh
    export INTEGRATION_TEST_CRED_PATH="projects/YOUR_PROJECT_ID/secrets/credentials/versions/latest"
    ```

## Build Docker Image

1. Update the [config.yaml](../config.yaml) file to point to the appropriate spanner database and port (9042).

2. Build the Docker image (build the Docker image every time the core proxy logic changes):
    ```sh
    docker build -t asia-south1-docker.pkg.dev/cassandra-to-spanner/spanner-adaptor-docker/spanner-adaptor:unit-test .
    ```

## Execute Integration Tests

1. Run the integration test cases with:
    ```sh
    # run with actual cassandra
    go test integration_test.go -args -target=cassandra

    # run with proxy
    go test integration_test.go -args -target=proxy
    ```