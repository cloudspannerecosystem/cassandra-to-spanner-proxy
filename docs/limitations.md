# Cassandra to Spanner Proxy - Limitations
## Overview
The Cassandra to Spanner Proxy is designed to help you migrate and integrate your Cassandra applications with Google Cloud Spanner, making this complex transition as smooth as possible. However, it's important to understand that this process isn't without its challenges. Some of the limitations you might encounter stem from the fundamental differences between Cassandra and Spanner's database architectures—think of these as mismatched puzzle pieces that don’t quite fit together. Other limitations exist simply because certain features haven’t been fully implemented yet in the proxy.

Our goal is to make this integration work as seamlessly as possible, but we want to be upfront about what’s currently possible and where you might hit some snags. This document walks you through the main limitations so you can plan ahead, work around them, or decide if additional customization is needed to meet your specific needs.

## 1. DDL support
The Cassandra to Spanner Proxy does not support Data Definition Language (DDL) queries. This means operations such as creating, altering, or dropping tables, indexes, or other schema modifications cannot be executed through the proxy. All DDL changes need to be managed directly in the respective database (Spanner) outside the proxy.

This limitation is primarily due to the significant differences in schema management and structure between Cassandra and Spanner, making direct translation complex and error-prone. For now, we recommend handling DDL operations manually within your database management workflow.


## 2. Supported Datatype

    | CQL Type              | Supported | Cloud Spanner Mapping   |
    |-----------------------|:---------:|:-----------------------:|
    | text                  |     ✓     |          STRING         |
    | blob                  |     ✓     |          BYTES          |
    | timestamp             |     ✓     |          TIMESTAMP      |
    | int                   |     ✓     |          INT64          |
    | bigint                |     ✓     |          INT64          |
    | float                 |     ✓     |          FLOAT64        |
    | double                |     ✓     |          FLOAT64        |
    | boolean               |     ✓     |          BOOL           |
    | uuid                  |     ✓     |          STRING         |
    | map<text, boolean>    |     ✓     |          JSON           |
    | map<text, text>       |     ✓     |          JSON           |
    | map<text, timestamp>  |     ✓     |          JSON           |
    | list<text>            |     ✓     |          ARRAY<STRING>  |
    | set<text>             |     ✓     |          ARRAY<STRING>  |

### Limitations of datatype: 

While we support complex data types such as lists, sets, and maps, our current implementation has limitations regarding complex update operations on these data types. Specifically, we do not support:

- Updating or adding new keys to a map. 
- Modifying existing keys in a map.
```  
UPDATE example_map 
SET data_map['new_key'] = 10 
WHERE id = some_uuid;
```
- Adding new values to a set.

```
UPDATE example_set 
SET data_set = data_set + {'new_value'}
WHERE id = some_uuid;
```

These limitations should be considered when designing and implementing operations involving complex data types in our system.


## 3. Supported Functions
    We are only supporting these functions as of now.

    - **round** - `select round(colx)  from keyspacex.tablex`
                    round with additional parameters not working like `round(colx, 2)`.
    - **count** - `"select count(colx)  from keyspacex.tablex.keyspaceX.tablex`
    - **writetime** - `select writetime(colx)  from keyspacex.tablex`


## 4. Queries with Literals
Due to limitations in the CQL grammar, you might encounter issues with certain column names that are treated as literals, such as `time`, `key`, `type`, and `json`. While we have added support for these specific keywords, there could still be cases where other literals cause conflicts. The good news is that the proxy is flexible, and we can easily extend support for additional keywords if needed. If you encounter any unexpected behavior with specific column names, reach out, and we’ll help you resolve it promptly.


## 5. Order By Queries

Currently, the proxy supports `ORDER BY` queries with only a single column. For example:

```sql
SELECT * FROM keyspace.table ORDER BY keyx;
```

If your queries involve ordering by multiple columns, this limitation might affect the query results or require rewriting your queries to fit within the current support scope. Enhancing support for multiple columns in `ORDER BY` clauses is on our roadmap, and we’re actively working to extend this functionality.

## 6. Partial Prepared Queries

Currently, the proxy does not support prepared queries where only some values are parameterized, while others are hardcoded. This means that queries where a mix of actual values and placeholders (`?`) are used in the same statement are not supported, except in the case of `LIMIT` clauses. Below are some examples to clarify:

- **Supported**: 
  ```sql
  INSERT INTO keyspace1.table1 (col1, col2, col3) VALUES (?, ?, ?);
  ```
- **Not Supported**:
  ```sql
  INSERT INTO keyspace1.table1 (col1, col2, col3) VALUES ('value', ?, ?);
  SELECT * FROM tableX WHERE col1='valueX' and col2 =?;
  ```

### Special Case for Select Queries with `LIMIT`
- **Supported**:
  ```sql
  SELECT * FROM keyspace1.table1 WHERE user_id = ? LIMIT ?;
  ```
- **Also Supported**:
  ```sql
  SELECT * FROM keyspace1.table1 WHERE user_id = ? LIMIT 5;
  ```

We aim to enhance support for partial prepared queries in future updates. For now, it's recommended to fully parameterize your queries or use literals consistently within the same statement.


## 7. Raw Queries in a batch is not supported

We do not support raw queries in batch 

**Not Supported**
```python
# Define the raw CQL queries
query1 = "INSERT INTO table1 (col1, col2, col3) VALUES ('value1', 'value2', 'value3');"
query2 = "INSERT INTO table1 (col1, col2, col3) VALUES ('value4', 'value5', 'value6');"
query3 = "UPDATE table1 SET col2 = 'updated_value' WHERE col1 = 'value1';"

# Create a BatchStatement
batch = BatchStatement()

# Add the raw queries to the batch
batch.add(SimpleStatement(query1))
batch.add(SimpleStatement(query2))
batch.add(SimpleStatement(query3))
```

```python
# Prepare the queries
insert_stmt = session.prepare("INSERT INTO table1 (col1, col2, col3) VALUES (?, ?, ?);")
update_stmt = session.prepare("UPDATE table1 SET col2 = ? WHERE col1 = ?;")

# Create a BatchStatement
batch = BatchStatement()

# Add the prepared statements to the batch with bound values
batch.add(insert_stmt, ('value1', 'value2', 'value3'))
batch.add(insert_stmt, ('value4', 'value5', 'value6'))
batch.add(update_stmt, ('updated_value', 'value1'))
```

## 8. CQlSH support
We have had limited support for cqlsh - [cqlsh support](./cqlsh.md)


## 9. Mandatory single quote surrounding values
- To run the Raw DML queries, it is mandatory for all values except numerics to have single quotes added to it. For eg.
    ```sh
    SELECT * FROM table WHERE name='john doe';
    INSERT INTO table (id, name) VALUES (1, 'john doe');
    ```

## 10. Complex datatype in where clause 
 Complex datatype in where clause are not supported.

```sql
    -- ids is of type map
    SELECT * FROM key_space.test_table WHERE ids CONTAINS False LIMIT 1

    -- sources is of type list
    SELECT * FROM key_space.test_table WHERE sources CONTAINS '4.0' LIMIT 1

```


## 11. Using timestamp 
### 11.1 Using timestamp with future timestamp
We do not support insert or update in future timestamp.

Example- not supported insert is happening in future
```python

future_timestamp = int(time.time() + 3600)  # Cassandra expects microseconds

query = f"""
INSERT INTO {table_name} {columns} VALUES ('value1', 'value2', 'value3') 
USING TIMESTAMP {future_timestamp};
"""

session.execute(SimpleStatement(query))

```
### 11.2 Using timestamp is maintained at row level instead of cell level
In our proxy setup with Spanner, we do not track the last updated timestamp for each individual cell. Instead, we maintain the last updated timestamp at the row level. This approach simplifies the tracking of changes but means that we capture updates based on the entire row rather than individual cell modifications.

This decision might come up with some limitations in application heavily relying on this feature of cassandra

## 12. Group By queries are not supported 
Group by queries are not supported

## 13. Limited support for system Queries
We only support limited [system Queries](https://docs.google.com/document/d/1kaziAycjxF8NHS-LzdKwjudt372g7pisolGdF6FG9JA/edit) .
 -  `SELECT * FROM system.peers_v2`
 -  `SELECT * FROM system.local WHERE key='local'`
 -  `SELECT * FROM system.peers`
 -  `SELECT * FROM system_schema.keyspaces`
 -  `SELECT * FROM system_schema.tables`
 -  `SELECT * FROM system_schema.columns`
 -  `SELECT * FROM system_schema.types`
 -  `SELECT * FROM system_schema.functions`
 -  `SELECT * FROM system_schema.aggregates`
 -  `SELECT * FROM system_schema.triggers`
 -  `SELECT * FROM system_schema.indexes`
 -  `SELECT * FROM system_schema.views`
 -  `SELECT * FROM system_virtual_schema.keyspaces`
 -  `SELECT * FROM system_virtual_schema.tables`
 -  `SELECT * FROM system_virtual_schema.columns`


