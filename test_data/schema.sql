/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
 
CREATE TABLE IF NOT EXISTS TableConfigurations ( `KeySpaceName` STRING(MAX), `TableName` STRING(MAX), `ColumnName` STRING(MAX), `ColumnType` STRING(MAX), `IsPrimaryKey` BOOL, `PK_Precedence` INT64) PRIMARY KEY (TableName, ColumnName, KeySpaceName);
CREATE TABLE IF NOT EXISTS keyspace1_comm_upload_event (`user_id` STRING(MAX),`upload_time` INT64,`comm_event` BYTES(MAX),spanner_ttl_ts TIMESTAMP  DEFAULT (NULL),last_commit_ts  TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true))primary key (`user_id`, `upload_time`),ROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts, INTERVAL 0 DAY));
CREATE TABLE IF NOT EXISTS keyspace1_address_book_entries (user_id STRING(MAX),  test_ids JSON,  spanner_ttl_ts TIMESTAMP DEFAULT (NULL), last_commit_ts  TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (user_id), ROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts, INTERVAL 0 DAY));
CREATE TABLE IF NOT EXISTS keyspace1_validate_data_types(text_col STRING(MAX), blob_col BYTES(MAX), timestamp_col timestamp, int_col int64, bigint_col int64, float_col float64, bool_col bool, uuid_col STRING(36), map_bool_col json, map_str_col json, map_ts_col json, list_str_col ARRAY<STRING(MAX)>, set_str_col ARRAY<STRING(MAX)>, spanner_ttl_ts TIMESTAMP DEFAULT (NULL), last_commit_ts  TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true))PRIMARY KEY (uuid_col DESC), ROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts, INTERVAL 0 DAY));
