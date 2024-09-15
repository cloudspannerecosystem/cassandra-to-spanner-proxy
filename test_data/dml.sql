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

INSERT INTO TableConfigurations (KeySpaceName, TableName, ColumnName, ColumnType, IsPrimaryKey, PK_Precedence) VALUES ('keyspace1', 'comm_upload_event', 'comm_event', 'blob', false, 0), ('keyspace1', 'comm_upload_event', 'upload_time', 'bigint', true, 2), ('keyspace1', 'comm_upload_event', 'user_id', 'text', true, 1);
INSERT INTO TableConfigurations (KeySpaceName, TableName, ColumnName, ColumnType, IsPrimaryKey, PK_Precedence) VALUES  ('keyspace1', 'address_book_entries', 'user_id', 'text', true, 1),('keyspace1', 'address_book_entries', 'test_ids', 'map<text, boolean>', false, 0);
INSERT INTO keyspace1_address_book_entries (user_id, test_ids, spanner_ttl_ts, last_commit_ts) values('user_idX', JSON'{\"kwy\": true}', null, CURRENT_TIMESTAMP );
INSERT INTO keyspace1_comm_upload_event (user_id, upload_time, comm_event, spanner_ttl_ts, last_commit_ts) VALUES ("some_user_id", 1623143245, CAST("some_event_data" AS BYTES), NULL, PENDING_COMMIT_TIMESTAMP());
INSERT INTO TableConfigurations (KeySpaceName, TableName, ColumnName, ColumnType, IsPrimaryKey, PK_Precedence) VALUES ('keyspace1', 'validate_data_types', 'text_col', 'text', false, 0), ('keyspace1', 'validate_data_types', 'blob_col','blob', FALSE, 0), ('keyspace1','validate_data_types',    'timestamp_col','timestamp',FALSE,0), ('keyspace1', 'validate_data_types','int_col', 'int',    FALSE, 0), ('keyspace1','validate_data_types','bigint_col','bigint',FALSE,0), ('keyspace1', 'validate_data_types', 'float_col','float',  FALSE, 0), ('keyspace1','validate_data_types','bool_col','boolean',FALSE, 0), ('keyspace1','validate_data_types','uuid_col','uuid', true,1), ('keyspace1','validate_data_types','map_bool_col','map<text, boolean>', FALSE,0), ('keyspace1','validate_data_types','map_str_col','map<text, text>', FALSE,0), ('keyspace1','validate_data_types','map_ts_col','map<text, timestamp>', FALSE,0), ('keyspace1','validate_data_types','list_str_col','list<text>', FALSE,0), ('keyspace1','validate_data_types','set_str_col','set<text>', FALSE,0);