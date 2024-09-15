# Differences from Cassandra

## WRITETIME function

In Cassandra a table contains the timestamp to represent date and time that a write operation was performed on a column or cell. The `WRITETIME()` function returns the timestamp of when a column was last written to (in milliseconds since the Unix epoch). **However, the proxy does not maintain column-level timestamps; instead, it tracks timestamps at the row level using the `last_commit_ts` column**. Consequently, if any column in a row is updated, the `WRITETIME()` function will return the updated timestamp for all columns in the row, regardless of whether they were modified or not.

For instance, the query `select WRITETIME(userid) from test.users` would return the last time the row was updated.  If the `firstname` column is subsequently updated, and the previous query is executed again, the returned timestamp would be the updated time, which may not be the expected behavior in Cassandra.

## INSERT ... IF NOT EXISTS keyword behavior

The proxy supports `IF NOT EXISTS` in the INSERT statements. Similar to Cassandra, Without `IF NOT EXISTS`, the command/query proceeds with no standard output. If `IF NOT EXISTS` returns true (if there is no row with this primary key), standard output displays a table like the following:

```
 [applied]
-----------------------
 True
```

If, however, the row does already exist, the command/query fails, and standard out displays a table with the value false in the `[applied]` column, (**however, unlike Cassandra, proxy will not show the values that were not inserted**), as in the following example:

```
 [applied]
-----------------------
 False
```