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

	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	cql "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/translator/cqlparser"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/utilities"
	"go.uber.org/zap"
)

func TestTranslator_ToSpannerUpdate(t *testing.T) {
	timeStamp, _ := utilities.FormatTimestamp(1709052458000212)

	type fields struct {
		Logger *zap.Logger
	}
	type args struct {
		query string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *UpdateQueryMap
		wantErr bool
	}{
		{
			name: "update success with raw query",
			args: args{
				query: "UPDATE key_space.test_table USING TTL 84600 AND TIMESTAMP 1709052458000212 SET column1 = 'testText' , column2 = '0x0000000000000003' WHERE column3='true';",
			},
			wantErr: false,
			want: &UpdateQueryMap{
				SpannerQuery: "UPDATE test_table SET `spanner_ttl_ts` = @ttlValue , `last_commit_ts` = @tsValue , `column1` = @set1 , `column2` = @set2 WHERE `column3` = @value1 AND `last_commit_ts` <= @tsValue;",
				ParamKeys:    []string{"ttlValue", "tsValue", "set1", "set2", "value1"},
				Params: map[string]interface{}{
					"ttlValue": utilities.AddSecondsToCurrentTimestamp(84600),
					"tsValue":  timeStamp,
					"set1":     "testText",
					"set2":     []byte(string("0x0000000000000003")),
					"value1":   true,
				},
				Keyspace: "key_space",
			},
		},
		{
			name: "update success with prepared query",
			args: args{
				query: "UPDATE key_space.test_table USING TTL ? AND TIMESTAMP ? SET column1 = ?, column2 = ? WHERE column3 = ?;",
			},
			wantErr: false,
			want: &UpdateQueryMap{
				SpannerQuery: "UPDATE test_table SET `spanner_ttl_ts` = @ttlValue , `last_commit_ts` = @tsValue , `column1` = @set1 , `column2` = @set2 WHERE `column3` = @value1 AND `last_commit_ts` <= @tsValue;",
				ParamKeys:    []string{"ttlValue", "tsValue", "set1", "set2", "value1"},
				Params:       map[string]interface{}{},
				Keyspace:     "key_space",
			},
		},
		{
			name: "update success with prepared query without where clause",
			args: args{
				query: "UPDATE key_space.test_table USING TIMESTAMP ? AND TTL ? SET column1 = ? , column2 = ?;",
			},
			wantErr: false,
			want: &UpdateQueryMap{
				SpannerQuery: "UPDATE test_table SET `last_commit_ts` = @tsValue , `spanner_ttl_ts` = @ttlValue , `column1` = @set1 , `column2` = @set2 WHERE `last_commit_ts` <= @tsValue;",
				ParamKeys:    []string{"tsValue", "ttlValue", "set1", "set2"},
				Params:       map[string]interface{}{},
				Keyspace:     "key_space",
			},
		},
		{
			name: "update success with raw query without USING TS",
			args: args{
				query: "UPDATE key_space.test_table USING TTL 84600 SET column1 = 'testText' , column2 = '0x0000000000000003' WHERE column3='true';",
			},
			wantErr: false,
			want: &UpdateQueryMap{
				SpannerQuery: "UPDATE test_table SET `spanner_ttl_ts` = @ttlValue , `last_commit_ts` = PENDING_COMMIT_TIMESTAMP() , `column1` = @set1 , `column2` = @set2 WHERE `column3` = @value1;",
				ParamKeys:    []string{"ttlValue", "set1", "set2", "value1"},
				Params: map[string]interface{}{
					"ttlValue": utilities.AddSecondsToCurrentTimestamp(84600),
					"set1":     "testText",
					"set2":     []byte(string("0x0000000000000003")),
					"value1":   true,
				},
				Keyspace: "key_space",
			},
		},
		{
			name: "update success with prepared query without USING TS",
			args: args{
				query: "UPDATE key_space.test_table USING TTL ? SET column1 = ? , column2 = ? WHERE column3 = ?;",
			},
			wantErr: false,
			want: &UpdateQueryMap{
				SpannerQuery: "UPDATE test_table SET `spanner_ttl_ts` = @ttlValue , `last_commit_ts` = PENDING_COMMIT_TIMESTAMP() , `column1` = @set1 , `column2` = @set2 WHERE `column3` = @value1;",
				ParamKeys:    []string{"ttlValue", "set1", "set2", "value1"},
				Params:       map[string]interface{}{},
				Keyspace:     "key_space",
			},
		},
		{
			name: "update success with raw query without USING TTL",
			args: args{
				query: "UPDATE key_space.test_table USING TIMESTAMP 1709052458000212 SET column1 = 'testText' , column2 = '0x0000000000000003' WHERE column3='true';",
			},
			wantErr: false,
			want: &UpdateQueryMap{
				SpannerQuery: "UPDATE test_table SET `last_commit_ts` = @tsValue , `column1` = @set1 , `column2` = @set2 WHERE `column3` = @value1 AND `last_commit_ts` <= @tsValue;",
				ParamKeys:    []string{"tsValue", "set1", "set2", "value1"},
				Params: map[string]interface{}{
					"tsValue": timeStamp,
					"set1":    "testText",
					"set2":    []byte(string("0x0000000000000003")),
					"value1":  true,
				},
				Keyspace: "key_space",
			},
		},
		{
			name: "update success with prepared query without USING TTL",
			args: args{
				query: "UPDATE key_space.test_table USING TIMESTAMP ? SET column1 = ? , column2 = ? WHERE column3 = ?;",
			},
			wantErr: false,
			want: &UpdateQueryMap{
				SpannerQuery: "UPDATE test_table SET `last_commit_ts` = @tsValue , `column1` = @set1 , `column2` = @set2 WHERE `column3` = @value1 AND `last_commit_ts` <= @tsValue;",
				ParamKeys:    []string{"tsValue", "set1", "set2", "value1"},
				Params:       map[string]interface{}{},
				Keyspace:     "key_space",
			},
		},
		{
			name: "update success with raw query without USING TS/TTL",
			args: args{
				query: "UPDATE key_space.test_table SET column1 = 'testText' , column2 = '0x0000000000000003' WHERE column3='true';",
			},
			wantErr: false,
			want: &UpdateQueryMap{
				SpannerQuery: "UPDATE test_table SET `column1` = @set1 , `column2` = @set2 , `last_commit_ts` = PENDING_COMMIT_TIMESTAMP() WHERE `column3` = @value1;",
				ParamKeys:    []string{"set1", "set2", "value1"},
				Params: map[string]interface{}{
					"set1":   "testText",
					"set2":   []byte(string("0x0000000000000003")),
					"value1": true,
				},
				Keyspace: "key_space",
			},
		},
		{
			name: "update for map column using key and value",
			args: args{
				query: "UPDATE key_space.test_table SET column8[?] = true WHERE column3 = ?;",
			},
			wantErr: false,
			want: &UpdateQueryMap{
				SpannerQuery: "UPDATE test_table SET `column8` = @map_key , `last_commit_ts` = PENDING_COMMIT_TIMESTAMP() WHERE `column3` = @value1;",
				ParamKeys:    []string{"map_key", "value1"},
				Params:       map[string]interface{}{},
				Keyspace:     "key_space",
			},
		},
		{
			name: "error when multiple map column update using key and value",
			args: args{
				query: "UPDATE key_space.test_table SET column8[?] = true AND column8[?] = false WHERE column3 = ?;",
			},
			wantErr: true,
		},
		{
			name: "error when map column update value is not raw",
			args: args{
				query: "UPDATE key_space.test_table SET column8[?] = ? WHERE column3 = ?;",
			},
			wantErr: true,
		},
		{
			name: "update success with prepared query without USING TS/TTL",
			args: args{
				query: "UPDATE key_space.test_table SET column1 = ? , column2 = ? WHERE column3 = ?;",
			},
			wantErr: false,
			want: &UpdateQueryMap{
				SpannerQuery: "UPDATE test_table SET `column1` = @set1 , `column2` = @set2 , `last_commit_ts` = PENDING_COMMIT_TIMESTAMP() WHERE `column3` = @value1;",
				ParamKeys:    []string{"set1", "set2", "value1"},
				Params:       map[string]interface{}{},
				Keyspace:     "key_space",
			},
		},
		{
			name: "update error with prepared query",
			args: args{
				query: "UPDATE SET column1 = ? , column2 = ? WHERE column3 = ?;",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableConfig := &tableConfig.TableConfig{
				Logger:          tt.fields.Logger,
				TablesMetaData:  mockTableConfig,
				PkMetadataCache: mockPkMetadata,
			}

			tr := &Translator{
				Logger:          tt.fields.Logger,
				TableConfig:     tableConfig,
				UseRowTimestamp: true,
				UseRowTTL:       true,
			}
			got, err := tr.ToSpannerUpdate(tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Translator.ToSpannerUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil && !reflect.DeepEqual(got.SpannerQuery, tt.want.SpannerQuery) {
				t.Errorf("Translator.ToSpannerUpdate() = %v, want %v", got.SpannerQuery, tt.want.SpannerQuery)
			}

			if got != nil && len(got.Params) > 0 && !reflect.DeepEqual(got.Params, tt.want.Params) {
				t.Errorf("Translator.ToSpannerUpdate() = %v, want %v", got.Params, tt.want.Params)
			}

			if got != nil && len(got.ParamKeys) > 0 && !reflect.DeepEqual(got.ParamKeys, tt.want.ParamKeys) {
				t.Errorf("Translator.ToSpannerUpdate() = %v, want %v", got.ParamKeys, tt.want.ParamKeys)
			}

			if got != nil && !reflect.DeepEqual(got.Keyspace, tt.want.Keyspace) {
				t.Errorf("Translator.ToSpannerUpdate() = %v, want %v", got.Keyspace, tt.want.Keyspace)
			}

		})
	}
}

func TestTranslator_ToSpannerUpdateWhenUsingTSTTLIsDisabled(t *testing.T) {
	timeStamp, _ := utilities.FormatTimestamp(1709052458000212)

	type fields struct {
		Logger *zap.Logger
	}
	type args struct {
		query string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *UpdateQueryMap
		wantErr bool
	}{
		{
			name: "update success with raw query Without TS & TTL",
			args: args{
				query: "UPDATE key_space.test_table SET column1 = 'testText' , column2 = '0x0000000000000003' WHERE column3='true';",
			},
			wantErr: false,
			want: &UpdateQueryMap{
				SpannerQuery: "UPDATE test_table SET `column1` = @set1 , `column2` = @set2 WHERE `column3` = @value1;",
				ParamKeys:    []string{"ttlValue", "tsValue", "set1", "set2", "value1"},
				Params: map[string]interface{}{
					"ttlValue": utilities.AddSecondsToCurrentTimestamp(84600),
					"tsValue":  timeStamp,
					"set1":     "testText",
					"set2":     []byte(string("0x0000000000000003")),
					"value1":   true,
				},
				Keyspace: "key_space",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableConfig := &tableConfig.TableConfig{
				Logger:          tt.fields.Logger,
				TablesMetaData:  mockTableConfig,
				PkMetadataCache: mockPkMetadata,
			}

			tr := &Translator{
				Logger:          tt.fields.Logger,
				TableConfig:     tableConfig,
				UseRowTimestamp: false,
				UseRowTTL:       false,
			}
			got, err := tr.ToSpannerUpdate(tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Translator.ToSpannerUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil && !reflect.DeepEqual(got.SpannerQuery, tt.want.SpannerQuery) {
				t.Errorf("Translator.ToSpannerUpdate() = %v, want %v", got.SpannerQuery, tt.want.SpannerQuery)
			}

		})
	}
}
func TestParseAssignments(t *testing.T) {
	// Mock table configuration

	tests := []struct {
		name             string
		assignments      string
		tableName        string
		IsMapUpdateByKey bool
		keyspace         string
		expectedResponse *UpdateSetResponse
		expectedErr      error
	}{
		{
			name:             "Valid assignments",
			assignments:      "column1 = 'test1', column2 = '0x0000000000000003'",
			tableName:        "test_table",
			IsMapUpdateByKey: false,
			keyspace:         "key_space",
			expectedResponse: &UpdateSetResponse{
				UpdateSetValues: []UpdateSetValue{
					{
						Column:           "column1",
						Value:            "@set1",
						IsMapUpdateByKey: false,
						RawValue:         "test1",
					},
					{
						Column:           "column2",
						Value:            "@set2",
						IsMapUpdateByKey: false,
						RawValue:         "0x0000000000000003",
					},
				},
				ParamKeys: []string{"set1", "set2"},
				Params:    map[string]interface{}{"set1": "test1", "set2": []byte(string("0x0000000000000003"))},
			},
			expectedErr: nil,
		},
		{
			name:             "Valid assignments for map column update by key",
			assignments:      "column8[?] = true",
			tableName:        "test_table",
			IsMapUpdateByKey: true,
			keyspace:         "key_space",
			expectedResponse: &UpdateSetResponse{
				UpdateSetValues: []UpdateSetValue{
					{
						Column:           "column8",
						Value:            "@map_key",
						IsMapUpdateByKey: true,
						RawValue:         "true",
						MapUpdateKey:     "?",
					},
				},
				ParamKeys: []string{mapKeyPlaceholder},
				Params:    map[string]interface{}{"map_key": "column1"},
			},
			expectedErr: nil,
		},
		{
			name:             "Empty assignments",
			assignments:      "",
			tableName:        "test_table",
			IsMapUpdateByKey: false,
			keyspace:         "key_space",
			expectedResponse: nil,
			expectedErr:      errors.New("invalid input"),
		},
	}

	tableConfig := &tableConfig.TableConfig{
		TablesMetaData:  mockTableConfig,
		PkMetadataCache: mockPkMetadata,
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var input []cql.IAssignmentElementContext
			if test.assignments != "" {
				p, _ := NewCqlParser(test.assignments, true)
				input = p.Assignments().AllAssignmentElement()
			}

			result, err := parseAssignments(input, test.tableName, tableConfig)

			// Validate error
			if (err == nil && test.expectedErr != nil) || (err != nil && test.expectedErr == nil) || (err != nil && test.expectedErr != nil && err.Error() != test.expectedErr.Error()) {
				t.Errorf("Expected error %v, but got %v", test.expectedErr, err)
			}

			// Validate response
			if result != nil && !reflect.DeepEqual(result.UpdateSetValues, test.expectedResponse.UpdateSetValues) {
				t.Errorf("Expected result %+v, but got %+v", test.expectedResponse.UpdateSetValues, result.UpdateSetValues)
			}
		})
	}
}

func TestGetSpannerUpdateQuery(t *testing.T) {
	tests := []struct {
		name          string
		data          *UpdateQueryMap
		expectedQuery string
	}{
		{
			name: "Valid update query with set values and where clause",
			data: &UpdateQueryMap{
				Table: "table_name",
				UpdateSetValues: []UpdateSetValue{
					{Column: "column1", Value: "@set1"},
					{Column: "column2", Value: "@set2"},
				},
				Clauses: []Clause{
					{Column: "id", Operator: "=", Value: "@value1"},
				},
			},
			expectedQuery: "UPDATE table_name SET `column1` = @set1 , `column2` = @set2 WHERE `id` = @value1;",
		},
		{
			name: "Valid update query with only set values",
			data: &UpdateQueryMap{
				Table: "table_name",
				UpdateSetValues: []UpdateSetValue{
					{Column: "column1", Value: "@set1"},
					{Column: "column2", Value: "@set2"},
				},
			},
			expectedQuery: "UPDATE table_name SET `column1` = @set1 , `column2` = @set2;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := createSpannerUpdateQuery(test.data.Table, test.data.UpdateSetValues, test.data.Clauses)

			// Validate query
			if result != test.expectedQuery {
				t.Errorf("Expected query %s, but got %s", test.expectedQuery, result)
			}
		})
	}
}
