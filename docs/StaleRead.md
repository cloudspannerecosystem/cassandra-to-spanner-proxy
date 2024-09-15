# Stale Read Support with Custom Payload in Cassandra

This documentation explains how to add support for stale reads using a custom payload in Cassandra queries. Stale reads allow querying data that might be slightly out of date, which can reduce the load on the database and improve performance for specific use cases where real-time consistency is not required.

## Custom Payload
 - Stale Read is allowed only for `prepared` `select` Query. 
 - `stale_time` Key: This key represents the maximum staleness, in seconds, that is acceptable for the query. When you set stale_time to `b'3'`, it indicates that you are willing to accept data that may be up to 3 seconds old. The value must be a byte-encoded string.
 - Byte-Encoding: The string value `'3'` is converted to bytes using the `b'3'` syntax. This is necessary because the custom payload values must be in byte format to be correctly interpreted by the Cassandra driver.



## Implementation

```python
from cassandra.cluster import Cluster
import traceback

class CassandraConnector:
    def __init__(self):
        # Connect to the Cassandra cluster
        self.cluster = Cluster(['127.0.0.1'], port=9043  )  # Adjust port if needed
        self.session = self.cluster.connect('xobni_derived')  # Replace with your keyspace name

    def execute_query_with_custom_payload(self):
        try:
            # Prepare a statement with custom payload
            query = "SELECT * FROM xobni_derived.upload_id_status WHERE guid = ?"  # Replace with your query
            prepared_statement = self.session.prepare(query)
            bound_statement = prepared_statement.bind(['8888'])

            # Create a custom payload
            custom_payload = {
                'spanner_maxstaleness': b'3s'  # Byte-encoded string value representing the maximum staleness acceptable (in seconds)
            }

            # Execute the query with custom payload
            result_set = self.session.execute(bound_statement, custom_payload=custom_payload)

            print("selected -> ", result_set.all())
            # Process the result
            for row in result_set:
                print(row['column_name'])  # Replace with your column name



        except Exception as e:
            print("An error occurred:", e)
            traceback.print_exc()

        finally:
            # Close the session and cluster connection
            self.session.shutdown()
            self.cluster.shutdown()

if __name__ == "__main__":
    connector = CassandraConnector()
    connector.execute_query_with_custom_payload()

```