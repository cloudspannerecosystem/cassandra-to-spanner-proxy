# CQL to Spanner Table Converter

This script converts Cassandra `CREATE TABLE` queries from a CQL file into Spanner DDL and applies the generated DDL on a specified Spanner database. It also creates a `TableConfigurations` table in Spanner to store metadata about each converted table.

## Features

- Converts Cassandra `CREATE TABLE` queries from a CQL file into Google Spanner `CREATE TABLE` queries.
- Executes the translated DDL on the specified Spanner database.
- Creates a `TableConfigurations` table to store details about each CQL query processed (if not already present).
- Offers an optional keyspace flattening flag for handling keyspaces in the converted schema.

## Requirements

- **Go**: Ensure that Go is installed on your system.
- **Google Cloud SDK**: Ensure `gcloud` is installed and authenticated with proper permissions.
- **Spanner Database**: You must have a Spanner instance and database ready to execute queries.

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/cloudspannerecosystem/cassandra-to-spanner-proxy.git
cd cassandra-to-spanner-proxy/schema_converter
```

### 2. Install Dependencies

Ensure that all necessary Go modules are installed:

```bash
go mod tidy
```

### 3. Set Up Google Cloud Credentials

Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your service account key file.

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-file.json"
```

### 4. Configure Spanner Details

Make sure to pass the necessary Spanner details in the script:
- **Project ID**: Google Cloud project containing the Spanner instance.
- **Instance ID**: Spanner instance to connect to.
- **Database ID**: Spanner database where the DDL will be applied.

## Usage

### 1. Running the Script

To run the script, pass the necessary flags along with a CQL file containing the `CREATE TABLE` queries. The script will convert them into Spanner-compatible DDL and execute them on the specified Spanner database. Additionally, you can enable or disable keyspace flattening using the `--keyspaceFlatter` flag.

```bash
go run cql_to_spanner_schema_converter.go --project <PROJECT_ID> --instance <INSTANCE_ID> --database <DATABASE_ID> --cql <PATH_TO_CQL_FILE> [--keyspaceFlatter]
```

- `<PROJECT_ID>`: Google Cloud project ID.
- `<INSTANCE_ID>`: Spanner instance ID.
- `<DATABASE_ID>`: Spanner database ID.
- `<PATH_TO_CQL_FILE>`: Path to the CQL file containing `CREATE TABLE` queries.
- `[--keyspaceFlatter]`: Optional flag to enable keyspace flattening in the conversion process.
- `[--table]`: Optional flag to specify different table name for TableConfigurations.
- `[--enableUsingTimestamp]`: Optional flag to enable 'Using Timestamp' features, default is false.
- `[--enableUsingTTL]`: Optional flag to enable 'Using TTL' features, default is false.
- `[--usePlainText]`: Optional flag to enable plain text connection for external host, default is false.
- `[--caCertificate]`: Optional flag to specify the CA certificate file path required for TLS/mTLS connection for external hosts.
- `[--clientCertificate]`: Optional flag to specify the client certificate file path required for mTLS connection for external hosts.
- `[--clientKey]`: Optional flag to specify the client key file path required for mTLS connection for external hosts.

### 2. Example Commands

#### With Keyspace Flattening

```bash
go run cql_to_spanner_schema_converter.go --project cassandra-to-spanner --instance spanner-instance-dev --database cluster10 --cql /path/to/cql-file.cql --keyspaceFlatter
```

In this mode, keyspaces from the CQL schema are flattened.

#### Without Keyspace Flattening

```bash
go run cql_to_spanner_schema_converter.go --project cassandra-to-spanner --instance spanner-instance-dev --database cluster10 --cql /path/to/cql-file.cql
```

In this mode, keyspaces are preserved in the schema.

### 3. Important Notes

- The script **does not drop** tables before creating them. Ensure the schema is adjusted accordingly if needed.
- The `TableConfigurations` table is created to track schema metadata.
- The script supports the same data types as the proxy adaptor, as mentioned in the [FAQs](https://github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/blob/develop/docs/faq.md#how-are-cql-data-types-mapped-with-cloud-spanner).

### Expected Output

The script logs details about each processed query, including any errors. Metadata about each translated table is stored in the `TableConfigurations` table and the spanner tables are created.