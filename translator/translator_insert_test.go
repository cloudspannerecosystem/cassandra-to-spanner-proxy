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
	"errors"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/ollionorg/cassandra-to-spanner-proxy/tableConfig"
	cql "github.com/ollionorg/cassandra-to-spanner-proxy/translator/cqlparser"
	"github.com/ollionorg/cassandra-to-spanner-proxy/utilities"
	"go.uber.org/zap"
)

func TestToSpannerUpsert(t *testing.T) {
	type fields struct {
		Logger *zap.Logger
	}
	type args struct {
		query string
	}

	inputQuery := `insert into key_space.test_table
	( column1, column2, column3, column4, column5, column6, column7, column8, column9)
	VALUES
	( 'test', '0x0000000000000003', 'true', '["The Parx Casino Philly Cycling Classic"]', '2015-05-03 13:30:54.234', '123', '{ "Rabobank-Liv Woman Cycling Team","Rabobank-Liv Giant","Rabobank Women Team","Nederland bloeit" }', '{i3=true, 5023.0=true}', '-10000000') USING TTL 86400 AND TIMESTAMP 1709037503000000;`

	inputPreparedQuery := `insert into key_space.test_table
	( column1, column2, column3, column4, column5, column6, column7, column8, column9)
	VALUES
	( ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? AND TIMESTAMP ?;`

	inputQueryTTLFirst := `insert into key_space.test_table
	( column1, column2, column3, column4, column5, column6, column7, column8, column9)
	VALUES
	( 'test', '0x0000000000000003', 'true', '["The Parx Casino Philly Cycling Classic"]', '2015-05-03 13:30:54.234', '123', '{ "Rabobank-Liv Woman Cycling Team","Rabobank-Liv Giant","Rabobank Women Team","Nederland bloeit" }', '{i3=true, 5023.0=true}', '-10000000') USING TIMESTAMP 1709037503000000 AND TTL 86400;`

	inputPreparedQueryTTLFirst := `insert into key_space.test_table
	( column1, column2, column3, column4, column5, column6, column7, column8, column9)
	VALUES
	( ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ? AND TTL ?;`

	inputQueryWithoutTS := `insert into key_space.test_table
	( column1, column2, column3, column4, column5, column6, column7, column8, column9)
	VALUES
	( 'test', '0x0000000000000003', 'true', '["The Parx Casino Philly Cycling Classic"]', '2015-05-03 13:30:54.234', '123', '{ "Rabobank-Liv Woman Cycling Team","Rabobank-Liv Giant","Rabobank Women Team","Nederland bloeit" }', '{i3=true, 5023.0=true}', '-10000000') USING TTL 86400;`

	inputPreparedQueryWithoutTS := `insert into key_space.test_table
	( column1, column2, column3, column4, column5, column6, column7, column8, column9)
	VALUES
	( ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?;`

	inputQueryWithoutTTL := `insert into key_space.test_table
	( column1, column2, column3, column4, column5, column6, column7, column8, column9)
	VALUES
	( 'test', '0x0000000000000003', 'true', '["The Parx Casino Philly Cycling Classic"]', '2015-05-03 13:30:54.234', '123', '{ "Rabobank-Liv Woman Cycling Team","Rabobank-Liv Giant","Rabobank Women Team","Nederland bloeit" }', '{i3=true, 5023.0=true}', '-10000000') USING TIMESTAMP 1709037503000000;`

	inputPreparedQueryWithoutTTL := `insert into key_space.test_table
	( column1, column2, column3, column4, column5, column6, column7, column8, column9)
	VALUES
	( ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?;`

	inputQueryWithoutTSTTL := `insert into key_space.test_table
	( column1, column2, column3, column4, column5, column6, column7, column8, column9)
	VALUES
	( 'test', '0x0000000000000003', 'true', '["The Parx Casino Philly Cycling Classic"]', '2015-05-03 13:30:54.234', '123', '{ "Rabobank-Liv Woman Cycling Team","Rabobank-Liv Giant","Rabobank Women Team","Nederland bloeit" }', '{i3=true, 5023.0=true}', '-10000000');`

	inputPreparedQueryWithoutTSTTL := `insert into key_space.test_table
	( column1, column2, column3, column4, column5, column6, column7, column8, column9)
	VALUES
	( ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	inputQueryWithIfNotExists := `insert into key_space.test_table
	( column1, column2, column3)
	VALUES
	( 'test', '0x0000000000000003', 'true') IF NOT EXISTS;`

	inputQueryWithIfNotExistsAndTS := `insert into key_space.test_table
	( column1, column2, column3)
	VALUES
	( 'test', '0x0000000000000003', 'true') IF NOT EXISTS USING TIMESTAMP 123456789;`

	tsCheck, _ := utilities.FormatTimestamp(1709037503000000)

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *InsertQueryMap
		wantErr bool
	}{
		{
			name: "test for raw query",
			args: args{
				query: inputQuery,
			},
			want: &InsertQueryMap{
				CassandraQuery: inputQuery,
				QueryType:      "insert",
				SpannerQuery:   "INSERT OR UPDATE INTO test_table (`column1`, `column2`, `column3`, `column4`, `column5`, `column6`, `column7`, `column8`, `column9`, `spanner_ttl_ts`, `last_commit_ts`) VALUES (@column1, @column2, @column3, @column4, @column5, @column6, @column7, @column8, @column9, @spanner_ttl_ts, @last_commit_ts);",
				Table:          "test_table",
				Keyspace:       "key_space",
				Params: map[string]interface{}{
					"column1": "test",
					"column2": []byte(string("0x0000000000000003")),
					"column3": true,
					"column4": []string{"The Parx Casino Philly Cycling Classic"},
					"column5": "2015-05-03 13:30:54.234",
					"column6": 123,
					"column7": []string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
					"column8": spanner.NullJSON{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true},
					"column9":        -10000000,
					"ttlValue":       utilities.AddSecondsToCurrentTimestamp(86400),
					"last_commit_ts": tsCheck,
				},
				ParamKeys: []string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "spanner_ttl_ts", "last_commit_ts"},
				Values: []interface{}{
					"test",
					[]byte(string("0x0000000000000003")),
					true,
					[]string{"The Parx Casino Philly Cycling Classic"},
					"2015-05-03 13:30:54.234",
					123,
					[]string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
					spanner.NullJSON{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true},
					-10000000,
					utilities.AddSecondsToCurrentTimestamp(86400),
					tsCheck,
				},
				UsingTSCheck: "SELECT last_commit_ts FROM test_table WHERE `column1` = @column1 AND `last_commit_ts` > @last_commit_ts;",
			},
			wantErr: false,
		},
		{
			name: "test for prepared query",
			args: args{
				query: inputPreparedQuery,
			},
			want: &InsertQueryMap{
				CassandraQuery: inputPreparedQuery,
				QueryType:      "insert",
				Keyspace:       "key_space",
				SpannerQuery:   "INSERT OR UPDATE INTO test_table (`column1`, `column2`, `column3`, `column4`, `column5`, `column6`, `column7`, `column8`, `column9`, `spanner_ttl_ts`, `last_commit_ts`) VALUES (@column1, @column2, @column3, @column4, @column5, @column6, @column7, @column8, @column9, @spanner_ttl_ts, @last_commit_ts);",
				Table:          "test_table",
				ParamKeys:      []string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "spanner_ttl_ts", "last_commit_ts"},
				UsingTSCheck:   "SELECT last_commit_ts FROM test_table WHERE `column1` = @column1 AND `last_commit_ts` > @last_commit_ts;",
			},
			wantErr: false,
		},
		{
			name: "test for raw query TTL first",
			args: args{
				query: inputQueryTTLFirst,
			},
			want: &InsertQueryMap{
				CassandraQuery: inputQueryTTLFirst,
				QueryType:      "insert",
				SpannerQuery:   "INSERT OR UPDATE INTO test_table (`column1`, `column2`, `column3`, `column4`, `column5`, `column6`, `column7`, `column8`, `column9`, `last_commit_ts`, `spanner_ttl_ts`) VALUES (@column1, @column2, @column3, @column4, @column5, @column6, @column7, @column8, @column9, @last_commit_ts, @spanner_ttl_ts);",
				Table:          "test_table",
				Keyspace:       "key_space",
				Params: map[string]interface{}{
					"column1": "test",
					"column2": []byte(string("0x0000000000000003")),
					"column3": true,
					"column4": []string{"The Parx Casino Philly Cycling Classic"},
					"column5": "2015-05-03 13:30:54.234",
					"column6": 123,
					"column7": []string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
					"column8": spanner.NullJSON{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true},
					"column9":        -10000000,
					"ttlValue":       utilities.AddSecondsToCurrentTimestamp(86400),
					"last_commit_ts": tsCheck,
				},
				ParamKeys: []string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "last_commit_ts", "spanner_ttl_ts"},
				Values: []interface{}{
					"test",
					[]byte(string("0x0000000000000003")),
					true,
					[]string{"The Parx Casino Philly Cycling Classic"},
					"2015-05-03 13:30:54.234",
					123,
					[]string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
					spanner.NullJSON{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true},
					-10000000,
					tsCheck,
					utilities.AddSecondsToCurrentTimestamp(86400),
				},
				UsingTSCheck: "SELECT last_commit_ts FROM test_table WHERE `column1` = @column1 AND `last_commit_ts` > @last_commit_ts;",
			},
			wantErr: false,
		},
		{
			name: "test for prepared query TTL first",
			args: args{
				query: inputPreparedQueryTTLFirst,
			},
			want: &InsertQueryMap{
				CassandraQuery: inputPreparedQueryTTLFirst,
				QueryType:      "insert",
				Keyspace:       "key_space",
				SpannerQuery:   "INSERT OR UPDATE INTO test_table (`column1`, `column2`, `column3`, `column4`, `column5`, `column6`, `column7`, `column8`, `column9`, `last_commit_ts`, `spanner_ttl_ts`) VALUES (@column1, @column2, @column3, @column4, @column5, @column6, @column7, @column8, @column9, @last_commit_ts, @spanner_ttl_ts);",
				Table:          "test_table",
				ParamKeys:      []string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "last_commit_ts", "spanner_ttl_ts"},
				UsingTSCheck:   "SELECT last_commit_ts FROM test_table WHERE `column1` = @column1 AND `last_commit_ts` > @last_commit_ts;",
			},
			wantErr: false,
		},
		{
			name: "test for raw query without Using TS With TTL",
			args: args{
				query: inputQueryWithoutTS,
			},
			want: &InsertQueryMap{
				CassandraQuery: inputQuery,
				QueryType:      "insert",
				SpannerQuery:   "INSERT OR UPDATE INTO test_table (`column1`, `column2`, `column3`, `column4`, `column5`, `column6`, `column7`, `column8`, `column9`, `last_commit_ts`, `spanner_ttl_ts`) VALUES (@column1, @column2, @column3, @column4, @column5, @column6, @column7, @column8, @column9, PENDING_COMMIT_TIMESTAMP(), @spanner_ttl_ts);",
				Table:          "test_table",
				Keyspace:       "key_space",
				Params: map[string]interface{}{
					"column1": "test",
					"column2": []byte(string("0x0000000000000003")),
					"column3": true,
					"column4": []string{"The Parx Casino Philly Cycling Classic"},
					"column5": "2015-05-03 13:30:54.234",
					"column6": 123,
					"column7": []string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
					"column8": spanner.NullJSON{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true},
					"column9":  -10000000,
					"ttlValue": utilities.AddSecondsToCurrentTimestamp(86400),
				},
				ParamKeys: []string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "last_commit_ts", "spanner_ttl_ts"},
				Values: []interface{}{
					"test",
					[]byte(string("0x0000000000000003")),
					true,
					[]string{"The Parx Casino Philly Cycling Classic"},
					"2015-05-03 13:30:54.234",
					123,
					[]string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
					spanner.NullJSON{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true},
					-10000000,
					spanner.CommitTimestamp,
					utilities.AddSecondsToCurrentTimestamp(86400),
				},
				UsingTSCheck: "",
			},
			wantErr: false,
		},
		{
			name: "test for prepared query Without TS with TTL",
			args: args{
				query: inputPreparedQueryWithoutTS,
			},
			want: &InsertQueryMap{
				CassandraQuery: inputPreparedQueryWithoutTS,
				QueryType:      "insert",
				Keyspace:       "key_space",
				SpannerQuery:   "INSERT OR UPDATE INTO test_table (`column1`, `column2`, `column3`, `column4`, `column5`, `column6`, `column7`, `column8`, `column9`, `spanner_ttl_ts`, `last_commit_ts`) VALUES (@column1, @column2, @column3, @column4, @column5, @column6, @column7, @column8, @column9, @spanner_ttl_ts, PENDING_COMMIT_TIMESTAMP());",
				Table:          "test_table",
				ParamKeys:      []string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "spanner_ttl_ts"},
				UsingTSCheck:   "",
			},
			wantErr: false,
		},
		{
			name: "test for raw query without Using TS/TTL",
			args: args{
				query: inputQueryWithoutTSTTL,
			},
			want: &InsertQueryMap{
				CassandraQuery: inputQueryWithoutTSTTL,
				QueryType:      "insert",
				SpannerQuery:   "INSERT OR UPDATE INTO test_table (`column1`, `column2`, `column3`, `column4`, `column5`, `column6`, `column7`, `column8`, `column9`, `last_commit_ts`) VALUES (@column1, @column2, @column3, @column4, @column5, @column6, @column7, @column8, @column9, PENDING_COMMIT_TIMESTAMP());",
				Table:          "test_table",
				Keyspace:       "key_space",
				Params: map[string]interface{}{
					"column1": "test",
					"column2": []byte(string("0x0000000000000003")),
					"column3": true,
					"column4": []string{"The Parx Casino Philly Cycling Classic"},
					"column5": "2015-05-03 13:30:54.234",
					"column6": 123,
					"column7": []string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
					"column8": spanner.NullJSON{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true},
					"column9": -10000000,
				},
				ParamKeys: []string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "last_commit_ts"},
				Values: []interface{}{
					"test",
					[]byte(string("0x0000000000000003")),
					true,
					[]string{"The Parx Casino Philly Cycling Classic"},
					"2015-05-03 13:30:54.234",
					123,
					[]string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
					spanner.NullJSON{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true},
					-10000000,
					spanner.CommitTimestamp,
				},
				UsingTSCheck: "",
			},
			wantErr: false,
		},
		{
			name: "test for prepared query Without TS/TTL",
			args: args{
				query: inputPreparedQueryWithoutTSTTL,
			},
			want: &InsertQueryMap{
				CassandraQuery: inputPreparedQueryWithoutTSTTL,
				QueryType:      "insert",
				Keyspace:       "key_space",
				SpannerQuery:   "INSERT OR UPDATE INTO test_table (`column1`, `column2`, `column3`, `column4`, `column5`, `column6`, `column7`, `column8`, `column9`, `last_commit_ts`) VALUES (@column1, @column2, @column3, @column4, @column5, @column6, @column7, @column8, @column9, PENDING_COMMIT_TIMESTAMP());",
				Table:          "test_table",
				ParamKeys:      []string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9"},
				UsingTSCheck:   "",
			},
			wantErr: false,
		},
		{
			name: "test for raw query without Using TTL",
			args: args{
				query: inputQueryWithoutTTL,
			},
			want: &InsertQueryMap{
				CassandraQuery: inputQueryWithoutTTL,
				QueryType:      "insert",
				SpannerQuery:   "INSERT OR UPDATE INTO test_table (`column1`, `column2`, `column3`, `column4`, `column5`, `column6`, `column7`, `column8`, `column9`, `last_commit_ts`) VALUES (@column1, @column2, @column3, @column4, @column5, @column6, @column7, @column8, @column9, @last_commit_ts);",
				Table:          "test_table",
				Keyspace:       "key_space",
				Params: map[string]interface{}{
					"column1": "test",
					"column2": []byte(string("0x0000000000000003")),
					"column3": true,
					"column4": []string{"The Parx Casino Philly Cycling Classic"},
					"column5": "2015-05-03 13:30:54.234",
					"column6": 123,
					"column7": []string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
					"column8": spanner.NullJSON{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true},
					"column9":        -10000000,
					"last_commit_ts": tsCheck,
				},
				ParamKeys: []string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "last_commit_ts"},
				Values: []interface{}{
					"test",
					[]byte(string("0x0000000000000003")),
					true,
					[]string{"The Parx Casino Philly Cycling Classic"},
					"2015-05-03 13:30:54.234",
					123,
					[]string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
					spanner.NullJSON{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true},
					-10000000,
					tsCheck,
				},
				UsingTSCheck: "SELECT last_commit_ts FROM test_table WHERE `column1` = @column1 AND `last_commit_ts` > @last_commit_ts;",
			},
			wantErr: false,
		},
		{
			name: "test for prepared query Without TTL",
			args: args{
				query: inputPreparedQueryWithoutTTL,
			},
			want: &InsertQueryMap{
				CassandraQuery: inputPreparedQueryWithoutTTL,
				QueryType:      "insert",
				Keyspace:       "key_space",
				SpannerQuery:   "INSERT OR UPDATE INTO test_table (`column1`, `column2`, `column3`, `column4`, `column5`, `column6`, `column7`, `column8`, `column9`, `last_commit_ts`) VALUES (@column1, @column2, @column3, @column4, @column5, @column6, @column7, @column8, @column9, @last_commit_ts);",
				Table:          "test_table",
				ParamKeys:      []string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "last_commit_ts"},
				UsingTSCheck:   "SELECT last_commit_ts FROM test_table WHERE `column1` = @column1 AND `last_commit_ts` > @last_commit_ts;",
				Params: map[string]interface{}{
					"column1": "test",
					"column2": []byte(string("0x0000000000000003")),
					"column3": true,
					"column4": []string{"The Parx Casino Philly Cycling Classic"},
					"column5": "2015-05-03 13:30:54.234",
					"column6": 123,
					"column7": []string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
					"column8": spanner.NullJSON{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true},
					"column9":        -10000000,
					"last_commit_ts": tsCheck,
				},
			},
			wantErr: false,
		},
		{
			name: "test query With IF NOT EXISTS",
			args: args{
				query: inputQueryWithIfNotExists,
			},
			want: &InsertQueryMap{
				CassandraQuery: inputQueryWithIfNotExists,
				QueryType:      "insert",
				SpannerQuery:   "INSERT OR UPDATE INTO test_table (`column1`, `column2`, `column3`, `last_commit_ts`) VALUES (@column1, @column2, @column3, PENDING_COMMIT_TIMESTAMP());",
				Table:          "test_table",
				Keyspace:       "key_space",
				Params: map[string]interface{}{
					"column1": "test",
					"column2": []byte(string("0x0000000000000003")),
					"column3": true,
				},
				ParamKeys: []string{"column1", "column2", "column3", "last_commit_ts"},
				Values: []interface{}{
					"test",
					[]byte(string("0x0000000000000003")),
					true,
					spanner.CommitTimestamp,
				},
				UsingTSCheck:   "",
				HasIfNotExists: true,
			},
			wantErr: false,
		},
		{
			name: "test query With IF NOT EXISTS and USING TIMESTAMP",
			args: args{
				query: inputQueryWithIfNotExistsAndTS,
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "error for table name",
			args: args{
				query: "INSERT OR UPDATE INTO table () VALUES ()",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "error for columns",
			args: args{
				query: "INSERT OR UPDATE INTO key_space.test_table () VALUES ()",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "error for values",
			args: args{
				query: "INSERT OR UPDATE INTO key_space.test_table (column1, column2) VALUES ()",
			},
			wantErr: true,
			want:    nil,
		},
	}
	for _, tt := range tests {
		tableConfig := &tableConfig.TableConfig{
			Logger:          tt.fields.Logger,
			TablesMetaData:  mockTableConfig,
			PkMetadataCache: mockPkMetadata,
		}
		t.Run(tt.name, func(t *testing.T) {
			tr := &Translator{
				Logger:      tt.fields.Logger,
				TableConfig: tableConfig,
			}
			got, err := tr.ToSpannerUpsert(tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Translator.ToSpannerUpsert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil && !reflect.DeepEqual(got.SpannerQuery, tt.want.SpannerQuery) {
				t.Errorf("Translator.ToSpannerUpsert() = %v, want %v", got.SpannerQuery, tt.want.SpannerQuery)
			}

			if got != nil && len(got.Params) > 0 && !reflect.DeepEqual(got.Params, tt.want.Params) {
				t.Errorf("Translator.ToSpannerUpsert() = %v, want %v", got.Params, tt.want.Params)
			}

			if got != nil && len(got.ParamKeys) > 0 && !reflect.DeepEqual(got.ParamKeys, tt.want.ParamKeys) {
				t.Errorf("Translator.ToSpannerUpsert() = %v, want %v", got.ParamKeys, tt.want.ParamKeys)
			}

			if got != nil && len(got.Values) > 0 && !reflect.DeepEqual(got.Values, tt.want.Values) {
				t.Errorf("Translator.ToSpannerUpsert() = %v, want %v", got.Values, tt.want.Values)
			}

			if got != nil && !reflect.DeepEqual(got.UsingTSCheck, tt.want.UsingTSCheck) {
				t.Errorf("Translator.ToSpannerUpsert() = %v, want %v", got.UsingTSCheck, tt.want.UsingTSCheck)
			}

			if got != nil && !reflect.DeepEqual(got.Keyspace, tt.want.Keyspace) {
				t.Errorf("Translator.ToSpannerUpsert() = %v, want %v", got.Keyspace, tt.want.Keyspace)
			}

			if got != nil && !reflect.DeepEqual(got.HasIfNotExists, tt.want.HasIfNotExists) {
				t.Errorf("Translator.ToSpannerUpsert() = %v, want %v", got.HasIfNotExists, tt.want.HasIfNotExists)
			}
		})
	}
}

func TestParseColumnsAndValuesFromInsert(t *testing.T) {

	tests := []struct {
		name                string
		input               string
		tableName           string
		keyspace            string
		expectedColumns     []Column
		expectedValueArr    []string
		expectedParamKeys   []string
		expectedPrimaryCols []string
		expectedErr         error
	}{
		{
			name:      "Valid input with columns",
			input:     "(column1, column2)",
			tableName: "test_table",
			keyspace:  "key_space",
			expectedColumns: []Column{
				{Name: "column1", SpannerType: "string", CQLType: "text"},
				{Name: "column2", SpannerType: "bytes", CQLType: "blob"},
			},
			expectedValueArr:    []string{"@column1", "@column2"},
			expectedParamKeys:   []string{"column1", "column2"},
			expectedPrimaryCols: []string{"column1"},
			expectedErr:         nil,
		},
		{
			name:      "Valid input with primary key columns",
			input:     "(column1)",
			tableName: "test_table",
			keyspace:  "key_space",
			expectedColumns: []Column{
				{Name: "column1", SpannerType: "string", CQLType: "text"},
			},
			expectedValueArr:    []string{"@column1"},
			expectedParamKeys:   []string{"column1"},
			expectedPrimaryCols: []string{"column1"},
			expectedErr:         nil,
		},
		{
			name:                "Invaid input with nil",
			input:               "",
			tableName:           "test_table",
			keyspace:            "key_space",
			expectedColumns:     nil,
			expectedValueArr:    nil,
			expectedParamKeys:   nil,
			expectedPrimaryCols: nil,
			expectedErr:         errors.New("parseColumnsAndValuesFromInsert: No Input parameters found for columns"),
		},
		{
			name:                "Invaid column name",
			input:               "(col1)",
			tableName:           "test_table",
			keyspace:            "key_space",
			expectedColumns:     nil,
			expectedValueArr:    nil,
			expectedParamKeys:   nil,
			expectedPrimaryCols: nil,
			expectedErr:         errors.New("parseColumnsAndValuesFromInsert: No Column type found in the Table Config"),
		},
		{
			name:                "empty column list",
			input:               "()",
			tableName:           "test_table",
			keyspace:            "key_space",
			expectedColumns:     nil,
			expectedValueArr:    nil,
			expectedParamKeys:   nil,
			expectedPrimaryCols: nil,
			expectedErr:         errors.New("parseColumnsAndValuesFromInsert: No Columns found in the Insert Query"),
		},
	}

	tableConfig := &tableConfig.TableConfig{
		TablesMetaData:  mockTableConfig,
		PkMetadataCache: mockPkMetadata,
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var input cql.IInsertColumnSpecContext
			if test.input == "" {
				input = nil
			} else {
				p, _ := NewCqlParser(test.input, true)
				input = p.InsertColumnSpec()

			}
			result, err := parseColumnsAndValuesFromInsert(input, test.tableName, tableConfig)

			// Validate error
			if !errorEquals(err, test.expectedErr) {
				t.Errorf("Expected error %v, but got %v", test.expectedErr, err)
			}

			// Validate result
			if result != nil {

				if !reflect.DeepEqual(result.Columns, test.expectedColumns) {
					t.Errorf("Expected result %+v, but got %+v", test.expectedColumns, result.Columns)
				}
				// Validate Columns
				if len(result.Columns) != len(test.expectedColumns) {
					t.Errorf("Expected %d columns, but got %d", len(test.expectedColumns), len(result.Columns))
				}

				// Validate ValueArr
				if len(result.ValueArr) != len(test.expectedValueArr) {
					t.Errorf("Expected %d value arr elements, but got %d", len(test.expectedValueArr), len(result.ValueArr))
				}

				// Validate ParamKeys
				if len(result.ParamKeys) != len(test.expectedParamKeys) {
					t.Errorf("Expected %d param keys elements, but got %d", len(test.expectedParamKeys), len(result.ParamKeys))
				}

				// Validate PrimaryColumns
				if len(result.PrimayColumns) != len(test.expectedPrimaryCols) {
					t.Errorf("Expected %d primary columns elements, but got %d", len(test.expectedPrimaryCols), len(result.PrimayColumns))
				}
			}
		})
	}
}

func Test_getSpannerInsertQuery(t *testing.T) {
	tests := []struct {
		name          string
		data          *InsertQueryMap
		expectedQuery string
		expectedErr   error
	}{
		{
			name: "success",
			data: &InsertQueryMap{
				Table:     "testTable",
				ParamKeys: []string{"column1"},
			},
			expectedQuery: "INSERT OR UPDATE INTO testTable (`column1`, `last_commit_ts`) VALUES (@column1, PENDING_COMMIT_TIMESTAMP());",
			expectedErr:   nil,
		},
		{
			name: "Valid input with table and param keys",
			data: &InsertQueryMap{
				Table:        "table_name",
				ParamKeys:    []string{"col1", "col2"},
				Values:       []interface{}{1, "value"},
				UsingTSCheck: "",
			},
			expectedQuery: "INSERT OR UPDATE INTO table_name (`col1`, `col2`) VALUES (@col1, @col2);",
			expectedErr:   nil,
		},
		{
			name: "Valid input with table, param keys, and using TS check",
			data: &InsertQueryMap{
				Table:        "table_name",
				ParamKeys:    []string{"col1", "col2"},
				Values:       []interface{}{1, "value"},
				UsingTSCheck: "timestamp",
			},
			expectedQuery: "INSERT OR UPDATE INTO table_name (`col1`, `col2`) VALUES (@col1, @col2);",
			expectedErr:   nil,
		},
		{
			name:          "Empty input",
			data:          &InsertQueryMap{},
			expectedQuery: "",
			expectedErr:   errors.New("one or more details not found to create Spanner query"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := getSpannerInsertQuery(test.data)

			// Validate error
			if !errorEquals(err, test.expectedErr) {
				t.Errorf("Expected error %v, but got %v", test.expectedErr, err)
			}

			// Validate query
			if result != test.expectedQuery {
				t.Errorf("Expected query %s, but got %s", test.expectedQuery, result)
			}
		})
	}
}

func TestGetTTLValue(t *testing.T) {
	tests := []struct {
		name        string
		ttlTsSpec   string
		expectedTTL string
		expectedErr error
	}{
		{
			name:        "Valid TTL value",
			ttlTsSpec:   "USING TTL 3600",
			expectedTTL: "3600",
			expectedErr: nil,
		},
		{
			name:        "Valid TTL value with placeholder",
			ttlTsSpec:   "USING TTL ?",
			expectedTTL: questionMark,
			expectedErr: nil,
		},
		{
			name:        "input as nil",
			ttlTsSpec:   "",
			expectedTTL: "",
			expectedErr: errors.New("invalid input"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var input cql.IUsingTtlTimestampContext
			if test.ttlTsSpec == "" {
				input = nil
			} else {
				p, _ := NewCqlParser(test.ttlTsSpec, true)
				input = p.UsingTtlTimestamp()

			}
			result, err := getTTLValue(input)

			// Validate error
			if !errorEquals(err, test.expectedErr) {
				t.Errorf("Expected error %v, but got %v", test.expectedErr, err)
			}

			// Validate TTL value
			if result != test.expectedTTL {
				t.Errorf("Expected TTL %s, but got %s", test.expectedTTL, result)
			}
		})
	}
}

func TestGetTimestampValue(t *testing.T) {
	tests := []struct {
		name        string
		ttlTsSpec   string
		expectedTTL string
		expectedErr error
	}{
		{
			name:        "Valid TTL value",
			ttlTsSpec:   "USING TIMESTAMP 1481124356754405",
			expectedTTL: "1481124356754405",
			expectedErr: nil,
		},
		{
			name:        "Valid TTL value with placeholder",
			ttlTsSpec:   "USING TIMESTAMP ?",
			expectedTTL: questionMark,
			expectedErr: nil,
		},
		{
			name:        "input as nil",
			ttlTsSpec:   "",
			expectedTTL: "",
			expectedErr: errors.New("invalid input"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var input cql.IUsingTtlTimestampContext
			if test.ttlTsSpec == "" {
				input = nil
			} else {
				p, _ := NewCqlParser(test.ttlTsSpec, true)
				input = p.UsingTtlTimestamp()

			}
			result, err := getTimestampValue(input)

			// Validate error
			if !errorEquals(err, test.expectedErr) {
				t.Errorf("Expected error %v, but got %v", test.expectedErr, err)
			}

			// Validate TTL value
			if result != test.expectedTTL {
				t.Errorf("Expected TTL %s, but got %s", test.expectedTTL, result)
			}
		})
	}
}

func TestFormatTSCheckQuery(t *testing.T) {
	tests := []struct {
		name        string
		primaryCols []string
		tableName   string
		expected    string
		expectedErr error
	}{
		{
			name:        "Valid parameters",
			primaryCols: []string{"id"},
			tableName:   "table1",
			expected:    "SELECT last_commit_ts FROM table1 WHERE `id` = @id AND `last_commit_ts` > @last_commit_ts;",
			expectedErr: nil,
		},
		{
			name:        "Empty primary keys",
			primaryCols: []string{},
			tableName:   "table1",
			expected:    "",
			expectedErr: errors.New("function needs atleast 1 primary key"),
		},
		{
			name:        "Missing table name",
			primaryCols: []string{"id"},
			tableName:   "",
			expected:    "",
			expectedErr: errors.New("table name is empty"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Call the formatTSCheckQuery function
			query, err := formatTSCheckQuery(test.primaryCols, test.tableName)

			// Check if the error matches the expected error
			if !errorEquals(err, test.expectedErr) {
				t.Errorf("Expected error: %v, got: %v", test.expectedErr, err)
			}

			// Check if the query matches the expected query
			if query != test.expected {
				t.Errorf("Expected query: %s, got: %s", test.expected, query)
			}
		})
	}
}

func TestSetParamsFromValues(t *testing.T) {
	columns := []Column{
		{Name: "id", SpannerType: "string", CQLType: "text"},
		{Name: "updated_at", SpannerType: "timestamp", CQLType: "timestamp"},
	}

	tests := []struct {
		name           string
		input          string
		expectedParams map[string]interface{}
		expectedValues []interface{}
		expectedErr    error
	}{
		{
			name:  "Valid input",
			input: "VALUES ('test_id', '2015-05-03 13:30:54.23')",
			expectedParams: map[string]interface{}{
				"id":         "test_id",
				"updated_at": "2015-05-03 13:30:54.23",
			},
			expectedValues: []interface{}{"test_id", "2015-05-03 13:30:54.23"},
			expectedErr:    nil,
		},
		{
			name:           "Empty input",
			input:          "",
			expectedParams: nil,
			expectedValues: nil,
			expectedErr:    errors.New("setParamsFromValues: No Value parameters found"),
		},
		{
			name:           "Invalid input",
			input:          "VALUES ('123')",
			expectedParams: nil,
			expectedValues: nil,
			expectedErr:    errors.New("setParamsFromValues: mismatch between columns and values"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var input cql.IInsertValuesSpecContext
			if test.input == "" {
				input = nil
			} else {
				p, _ := NewCqlParser(test.input, true)
				input = p.InsertValuesSpec()

			}
			// Call the setParamsFromValues function
			params, values, err := setParamsFromValues(input, columns)

			// Check if the error matches the expected error
			if (err != nil && test.expectedErr == nil) || (err == nil && test.expectedErr != nil) || (err != nil && test.expectedErr != nil && err.Error() != test.expectedErr.Error()) {
				t.Errorf("Expected error: %v, got: %v", test.expectedErr, err)
			}

			// Check if the params match the expected params
			if !reflect.DeepEqual(params, test.expectedParams) {
				t.Errorf("Expected result %+v, but got %+v", test.expectedParams, params)
			}

			// Check if the params match the expected params
			if !reflect.DeepEqual(values, test.expectedValues) {
				t.Errorf("Expected result %+v, but got %+v", test.expectedValues, values)
			}
		})
	}
}
