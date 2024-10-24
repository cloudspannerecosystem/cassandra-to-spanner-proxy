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
package translator

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToSpannerCreate(t *testing.T) {
	ttl_val_seconds := int64(432000)               // 432000 seconds equals 30 days.
	ttl_val_days := ttl_val_seconds / SecondsInDay // spanner query expects in days.
	tests := []struct {
		name                 string
		query                string
		expectedQuery        string
		expectError          bool
		keyspaceFlattening   bool
		enableUsingTimestamp bool
		enableUsingTTL       bool
	}{
		{
			name:                 "IF NOT EXISTS with keyspace in the query",
			query:                fmt.Sprintf("CREATE TABLE IF NOT EXISTS keyspace1.table1 (id int, name text, PRIMARY KEY (id)) WITH default_time_to_live = %s;", strconv.FormatInt(ttl_val_seconds, 10)),
			expectedQuery:        fmt.Sprintf("CREATE TABLE IF NOT EXISTS keyspace1_table1 (\n\t`id` INT64,\n\t`name` STRING(MAX),\n\tspanner_ttl_ts TIMESTAMP DEFAULT (NULL),\n\tlast_commit_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)\n) PRIMARY KEY (`id`),\nROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts, INTERVAL %s DAY))", strconv.FormatInt(ttl_val_days, 10)),
			expectError:          false,
			keyspaceFlattening:   true,
			enableUsingTimestamp: true,
			enableUsingTTL:       true,
		},
		{
			name:                 "IF NOT EXISTS without keyspace flattening",
			query:                fmt.Sprintf("CREATE TABLE IF NOT EXISTS keyspace1.table1 (id int, name text, PRIMARY KEY (id)) WITH default_time_to_live = %s;", strconv.FormatInt(ttl_val_seconds, 10)),
			expectedQuery:        fmt.Sprintf("CREATE TABLE IF NOT EXISTS table1 (\n\t`id` INT64,\n\t`name` STRING(MAX),\n\tspanner_ttl_ts TIMESTAMP DEFAULT (NULL),\n\tlast_commit_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)\n) PRIMARY KEY (`id`),\nROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts, INTERVAL %s DAY))", strconv.FormatInt(ttl_val_days, 10)),
			expectError:          false,
			keyspaceFlattening:   false,
			enableUsingTimestamp: true,
			enableUsingTTL:       true,
		},
		{
			name:                 "Without IF NOT EXISTS with keyspace in the query",
			query:                fmt.Sprintf("CREATE TABLE keyspace1.table1 (id int, name text, PRIMARY KEY (id)) WITH default_time_to_live = %s;", strconv.FormatInt(ttl_val_seconds, 10)),
			expectedQuery:        fmt.Sprintf("CREATE TABLE keyspace1_table1 (\n\t`id` INT64,\n\t`name` STRING(MAX),\n\tspanner_ttl_ts TIMESTAMP DEFAULT (NULL),\n\tlast_commit_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)\n) PRIMARY KEY (`id`),\nROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts, INTERVAL %s DAY))", strconv.FormatInt(ttl_val_days, 10)),
			expectError:          false,
			keyspaceFlattening:   true,
			enableUsingTimestamp: true,
			enableUsingTTL:       true,
		},
		{
			name:                 "IF NOT EXISTS without keyspace",
			query:                fmt.Sprintf("CREATE TABLE IF NOT EXISTS table1 (id int, name text, PRIMARY KEY (id)) WITH default_time_to_live = %s;", strconv.FormatInt(ttl_val_seconds, 10)),
			expectedQuery:        fmt.Sprintf("CREATE TABLE IF NOT EXISTS table1 (\n\t`id` INT64,\n\t`name` STRING(MAX),\n\tspanner_ttl_ts TIMESTAMP DEFAULT (NULL),\n\tlast_commit_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)\n) PRIMARY KEY (`id`),\nROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts, INTERVAL %s DAY))", strconv.FormatInt(ttl_val_days, 10)),
			expectError:          true,
			enableUsingTimestamp: true,
			enableUsingTTL:       true,
		},
		{
			name:                 "Without IF NOT EXISTS without keyspace",
			query:                fmt.Sprintf("CREATE TABLE table1 (id int, name text,  PRIMARY KEY (id)) WITH default_time_to_live = %s;", strconv.FormatInt(ttl_val_seconds, 10)),
			expectedQuery:        fmt.Sprintf("CREATE TABLE table1 (\n\t`id` INT64,\n\t`name` STRING(MAX),\n\tspanner_ttl_ts TIMESTAMP DEFAULT (NULL),\n\tlast_commit_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)\n) PRIMARY KEY (`id`),\nROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts, INTERVAL %s DAY))", strconv.FormatInt(ttl_val_days, 10)),
			expectError:          true,
			enableUsingTimestamp: true,
			enableUsingTTL:       true,
		},
		{
			name:                 "Zero seconds ttl value",
			query:                "CREATE TABLE IF NOT EXISTS keyspace1.table1 (id int, name text, PRIMARY KEY (id)) WITH default_time_to_live = 0;",
			expectedQuery:        "CREATE TABLE IF NOT EXISTS keyspace1_table1 (\n\t`id` INT64,\n\t`name` STRING(MAX),\n\tspanner_ttl_ts TIMESTAMP DEFAULT (NULL),\n\tlast_commit_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)\n) PRIMARY KEY (`id`),\nROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts, INTERVAL 0 DAY))",
			expectError:          false,
			keyspaceFlattening:   true,
			enableUsingTimestamp: true,
			enableUsingTTL:       true,
		},
		{
			name:                 "if enableUsingTimestamp is false",
			query:                "CREATE TABLE IF NOT EXISTS keyspace1.table1 (id int, name text, PRIMARY KEY (id)) WITH default_time_to_live = 0;",
			expectedQuery:        "CREATE TABLE IF NOT EXISTS keyspace1_table1 (\n\t`id` INT64,\n\t`name` STRING(MAX),\n\tspanner_ttl_ts TIMESTAMP DEFAULT (NULL)\n) PRIMARY KEY (`id`),\nROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts, INTERVAL 0 DAY))",
			expectError:          false,
			keyspaceFlattening:   true,
			enableUsingTimestamp: false,
			enableUsingTTL:       true,
		},
		{
			name:                 "if enableUsingTTL is false",
			query:                "CREATE TABLE IF NOT EXISTS keyspace1.table1 (id int, name text, PRIMARY KEY (id)) WITH default_time_to_live = 0;",
			expectedQuery:        "CREATE TABLE IF NOT EXISTS keyspace1_table1 (\n\t`id` INT64,\n\t`name` STRING(MAX),\n\tlast_commit_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)\n) PRIMARY KEY (`id`)",
			expectError:          false,
			keyspaceFlattening:   true,
			enableUsingTimestamp: true,
			enableUsingTTL:       false,
		},
		{
			name:                 "if enableUsingTimestamp & enableUsingTTL is false",
			query:                "CREATE TABLE IF NOT EXISTS keyspace1.table1 (id int, name text, PRIMARY KEY (id)) WITH default_time_to_live = 0;",
			expectedQuery:        "CREATE TABLE IF NOT EXISTS keyspace1_table1 (\n\t`id` INT64,\n\t`name` STRING(MAX)\n) PRIMARY KEY (`id`)",
			expectError:          false,
			keyspaceFlattening:   true,
			enableUsingTimestamp: false,
			enableUsingTTL:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := &Translator{}
			result, err := translator.ToSpannerCreate(tt.query, tt.keyspaceFlattening, tt.enableUsingTimestamp, tt.enableUsingTTL)

			if tt.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			// Assert the generated Spanner query
			assert.Equal(t, tt.expectedQuery, result.SpannerQuery)
		})
	}
}
