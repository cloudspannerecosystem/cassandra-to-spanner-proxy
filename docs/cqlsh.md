# Integrating CQLSH with Cassandra-to-Spanner Proxy

This guide provides detailed instructions on setting up CQLSH to connect to the Cassandra-to-Spanner proxy, along with an overview of supported and unsupported queries, and known limitations.


## Prerequisites

- **CQLSH Version:** Ensure you have CQLSH versions from the branches `cassandra-4.0.13` or `cassandra-4.1.5`.
- **Proxy Setup:** Ensure the Cassandra-to-Spanner proxy is set up and running.

## Setup Instructions

>> NOTE: You may skip these instructions, if you already have CQLSH interface installed.

### Option 1: Install CQLSH Directly

#### Step 1: Download CQLSH

Download the appropriate version of CQLSH by cloning the repository and following the instructions:

- **CQLSH 4.0.13:**
  ```sh
  git clone https://github.com/apache/cassandra.git -b cassandra-4.0.13
  cd cassandra/bin
  ```

- **CQLSH 4.1.5:**
  ```sh
  git clone https://github.com/apache/cassandra.git -b cassandra-4.1.5
  cd cassandra/bin
  ```

#### Step 2: Configure CQLSH to Connect to the Proxy (Optional)

Edit the CQLSH configuration to point to the Cassandra-to-Spanner proxy:

1. Open the `cqlshrc` configuration file. If it does not exist, create one in your home directory:
   ```sh
   nano ~/.cassandra/cqlshrc
   ```

2. Add the following configuration:
   ```ini
   [connection]
   hostname = <proxy_hostname>
   port = <proxy_port>
   ```

   Replace `<proxy_hostname>` and `<proxy_port>` with the appropriate values for your proxy setup.

#### Step 3: Launch CQLSH

Launch CQLSH with the configured/default settings:
```sh
./cqlsh
```

Launch CQLSH with the custom hostname and port:
```sh
./cqlsh <proxy_hostname> <proxy_port>
```

Replace `<proxy_hostname>` and `<proxy_port>` with the appropriate values.

### Option 2: Use Dockerized CQLSH

#### Step 1: Install Docker

Ensure Docker is installed on your machine. Follow the instructions on the [Docker website](https://docs.docker.com/get-docker/) to install Docker for your operating system.

#### Step 2: Download and Run the Dockerized CQLSH

Download the relevant Docker image and open a bash shell:
```sh
docker run -it nuvo/docker-cqlsh bash
```

#### Step 3: Find Your Machine’s IP Address

Find the local IP address of the machine if the proxy is running locally, or use the proxy server IP address. For macOS, you can get the local machine IP address using:
```sh
ifconfig | grep "inet " | grep -v 127.0.0.1
```

#### Step 4: Connect to the Proxy

Open a bash shell in the Docker image:
```sh
docker run -it nuvo/docker-cqlsh bash
```

Connect to the proxy using your IP address and port:
```sh
cqlsh --cqlversion='3.4.5' '<your_ip_address>'
```

Replace `<your_ip_address>` with the local IP address obtained in Step 3.

## Supported Queries

The following queries are supported by the Cassandra-to-Spanner proxy when using CQLSH:

### Basic CRUD Operations

**Insert:**
```sql
INSERT INTO keyspace_name.table_name (col1, col2, time, count)
  VALUES ('1234', 'check', '2024-06-13T05:19:16.882Z', 10);

// Inserting a row only if it does not already exist
INSERT INTO keyspace_name.table_name (id, lastname, firstname)
   VALUES (c4b65263-fe58-4846-83e8-f0e1c13d518f, 'John', 'Rissella')
IF NOT EXISTS;
```

**Select & limit:**
```sql
SELECT * FROM keyspace_name.table_name WHERE col1 = '1234';
SELECT * FROM keyspace_name.table_name limit 2;
```

**Update:**
```sql
UPDATE keyspace_name.table_name SET count = 15 WHERE col1 = '1234' AND col2 = 'check' AND time = '2024-06-13T05:19:16.882Z';
```

**Delete:**
```sql
DELETE FROM keyspace_name.table_name WHERE col1 = '1234' AND col2 = 'check';
```

**Using timestamp :**
```sql
 INSERT INTO keyspace_name.table_name (col1, col2, col3) VALUES ('Value1', '3', test_blob_data) USING TIMESTAMP 1686638356882;

 UPDATE keyspace_name.table_name using timestamp 1686638356882 SET count = 2, last_update_time = '2024-06-13T05:19:16.882Z' WHERE col1 = 'Value1'

 DELETE FROM keyspace_name.table_name USING TIMESTAMP 1686638356882 WHERE guid='Value1';

```

**Using ttl :**
```sql
 INSERT INTO keyspace_name.table_name (col1, col2, col3)
  VALUES ('col_val', '33233', 'test')
USING TTL 100 and timestamp 1686638356882;

 UPDATE keyspace_name.table_name using timestamp 1686638356882 and tll 100
  SET count = 2, last_update_time = '2024-06-13T05:19:16.882Z' WHERE col1 = 'Value1';
```
**Allow filtering :**
```sql
SELECT * FROM keyspace_name.table_name WHERE col3>= 10 and col3<= 1000 limit 10 allow filtering ;
```


## Unsupported Queries

The following types of queries are not supported by the Cassandra-to-Spanner proxy when using CQLSH:

### System Queries
    System Queries using cqlsh is not supported
- **Fetching Keyspaces:**
  ```sql
  SELECT * FROM system_schema.keyspaces;
  ```

- **Fetching Tables:**
  ```sql
  SELECT * FROM system_schema.tables;
  ```

- **Fetching Columns:**
  ```sql
  SELECT * FROM system_schema.columns;
  ```

- ### Batch Queries
    Raw batch queries are not yet supported (the support is coming soon!)

    ```sql
    BEGIN BATCH
    INSERT INTO keyspace_name.table_name (col1, col2, time, count) VALUES ('1234', 'check', '2024-06-13T05:19:16.882Z', 10);
    INSERT INTO keyspace_name.table_name (col1, col2, time, count) VALUES ('1234', 'check', '2024-06-13T05:19:16.882Z', 20);
    APPLY BATCH;
    ```

- ## DDL Queries
 DDL queries are not supported
- **Create Table:**
  ```sql
  CREATE TABLE keyspace_name.table_name (id UUID PRIMARY KEY, name text);
  ```

- **Drop Table:**
  ```sql
  DROP TABLE keyspace_name.table_name;
  ```

- **Describe Table:**
  ```sql
  DESCRIBE TABLE keyspace_name.table_name;
  ```

## Additional Notes

- **Timestamp Format:** Queries should contain timestamp values in the format `2024-06-13T05:19:16.882Z`.

  Example:
  ```sql
  INSERT INTO keyspace_name.table_name (col1, time, col3) VALUES ('Value1', '2024-06-13T05:19:16.882Z', ['test', 'test1']);
  ```
- **Complex Update :**  Queries trying to add or remove given value from a map, set or array is `not supported`. 
    ```sql
    UPDATE keyspace_name.table_name USING TTL 10 AND TIMESTAMP 1686638356882 SET col3['key'] = false WHERE col1 = 'col1_val' AND col2 = 'xyz';
    ```
