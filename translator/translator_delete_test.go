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
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/ollionorg/cassandra-to-spanner-proxy/tableConfig"
	"github.com/ollionorg/cassandra-to-spanner-proxy/utilities"
	"go.uber.org/zap"
)

func TestTranslator_ToSpannerDelete(t *testing.T) {
	tsCheck, _ := utilities.FormatTimestamp(1709052458000212)
	type fields struct {
		Logger *zap.Logger
	}
	type args struct {
		query string
	}

	inputQuery := `delete from key_space.test_table USING TIMESTAMP 1709052458000212 
	where column1 = 'test' AND column2 < '0x0000000000000003' AND column3='true'
	AND column4='["The Parx Casino Philly Cycling Classic"]' AND column5 <= '2015-05-03 13:30:54.234' AND column6 >= '123'
	AND column7='{ "Rabobank-Liv Woman Cycling Team","Rabobank-Liv Giant","Rabobank Women Team","Nederland bloeit" }' AND column8='{i3=true, 5023.0=true}'
	AND column9 > '-10000000' AND column10 = '1.12' AND column11 = '{key1=abc, key2=abc}'`

	inputPreparedQuery := `delete from key_space.test_table USING TIMESTAMP ?
	where column1 = ? AND column2=? AND column3=? 
	AND column4=? AND column5=? AND column6=? 
	AND column7=? AND column8=? 
	AND column9=?;`

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *DeleteQueryMap
		wantErr bool
	}{
		{
			name: "success for raw query",
			args: args{
				query: inputQuery,
			},
			want: &DeleteQueryMap{
				CassandraQuery: inputQuery,
				QueryType:      "delete",
				Table:          "test_table",
				Keyspace:       "key_space",
				SpannerQuery:   "DELETE FROM test_table WHERE `last_commit_ts` <= @tsValue AND `column1` = @value1 AND `column2` < @value2 AND `column3` = @value3 AND `column4` = @value4 AND `column5` <= @value5 AND `column6` >= @value6 AND `column7` = @value7 AND `column8` = @value8 AND `column9` > @value9 AND `column10` = @value10 AND `column11` = @value11;",
				Params: map[string]interface{}{
					"tsValue": tsCheck,
					"value1":  "test",
					"value2":  []byte(string("0x0000000000000003")),
					"value3":  true,
					"value4":  []string{"The Parx Casino Philly Cycling Classic"},
					"value5":  "2015-05-03 13:30:54.234",
					"value6":  123,
					"value7":  []string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
					"value8": spanner.NullJSON{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true},
					"value9":  -10000000,
					"value10": float64(1.12),
					"value11": spanner.NullJSON{Value: map[string]interface{}{
						"key1": "abc",
						"key2": "abc",
					}, Valid: true},
				},
				ParamKeys: []string{"tsValue", "value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10", "value11"},
				Clauses: []Clause{
					{
						Column:   "last_commit_ts",
						Operator: "<=",
						Value:    "@tsValue",
					},
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
					{
						Column:   "column2",
						Operator: "<",
						Value:    "@value2",
					},
					{
						Column:   "column3",
						Operator: "=",
						Value:    "@value3",
					},
					{
						Column:   "column4",
						Operator: "=",
						Value:    "@value4",
					},
					{
						Column:   "column5",
						Operator: "<=",
						Value:    "@value5",
					},
					{
						Column:   "column6",
						Operator: ">=",
						Value:    "@value6",
					},
					{
						Column:   "column7",
						Operator: "=",
						Value:    "@value7",
					},
					{
						Column:   "column8",
						Operator: "=",
						Value:    "@value8",
					},
					{
						Column:   "column9",
						Operator: ">",
						Value:    "@value9",
					},
					{
						Column:   "column10",
						Operator: "=",
						Value:    "@value10",
					},
					{
						Column:   "column11",
						Operator: "=",
						Value:    "@value11",
					},
				},
				PrimaryKeys: []string{"column1"},
			},
			wantErr: false,
		},
		{
			name: "success for prepared query",
			args: args{
				query: inputPreparedQuery,
			},
			want: &DeleteQueryMap{
				CassandraQuery: inputPreparedQuery,
				QueryType:      "delete",
				Table:          "test_table",
				Keyspace:       "key_space",
				SpannerQuery:   "DELETE FROM test_table WHERE `last_commit_ts` <= @tsValue AND `column1` = @value1 AND `column2` = @value2 AND `column3` = @value3 AND `column4` = @value4 AND `column5` = @value5 AND `column6` = @value6 AND `column7` = @value7 AND `column8` = @value8 AND `column9` = @value9;",
				Params:         map[string]interface{}{},
				ParamKeys:      []string{"tsValue", "value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9"},
				Clauses: []Clause{
					{
						Column:   "last_commit_ts",
						Operator: "<=",
						Value:    "@tsValue",
					},
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
					{
						Column:   "column2",
						Operator: "=",
						Value:    "@value2",
					},
					{
						Column:   "column3",
						Operator: "=",
						Value:    "@value3",
					},
					{
						Column:   "column4",
						Operator: "=",
						Value:    "@value4",
					},
					{
						Column:   "column5",
						Operator: "=",
						Value:    "@value5",
					},
					{
						Column:   "column6",
						Operator: "=",
						Value:    "@value6",
					},
					{
						Column:   "column7",
						Operator: "=",
						Value:    "@value7",
					},
					{
						Column:   "column8",
						Operator: "=",
						Value:    "@value8",
					},
					{
						Column:   "column9",
						Operator: "=",
						Value:    "@value9",
					},
				},
				PrimaryKeys: []string{"column1"},
			},
			wantErr: false,
		},
		{
			name: "success for query with IN operator",
			args: args{
				query: "delete from key_space.test_table where column1 IN ('test', 'test2')",
			},
			want: &DeleteQueryMap{
				QueryType:    "delete",
				SpannerQuery: "DELETE FROM test_table WHERE `column1` IN UNNEST(@value1);",
				Table:        "test_table",
				Keyspace:     "key_space",
				Params: map[string]interface{}{
					"value1": []string{"test", "test2"},
				},
				ParamKeys: []string{"value1"},
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "IN",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
				},
				PrimaryKeys: []string{"column1"},
			},
			wantErr: false,
		},
		{
			name: "query with no clauses",
			args: args{
				query: "delete from key_space.test_table",
			},
			want: &DeleteQueryMap{
				QueryType:    "delete",
				Table:        "test_table",
				Keyspace:     "key_space",
				SpannerQuery: "DELETE FROM test_table;",
				Params:       map[string]interface{}{},
				ParamKeys:    []string{},
				PrimaryKeys:  []string{"column1"},
			},
			wantErr: false,
		},
		{
			name: "query with single clause",
			args: args{
				query: "delete from key_space.test_table WHERE column1 = 'test'",
			},
			want: &DeleteQueryMap{
				QueryType:    "delete",
				Table:        "test_table",
				Keyspace:     "key_space",
				SpannerQuery: "DELETE FROM test_table WHERE `column1` = @value1;",
				Params: map[string]interface{}{
					"value1": "test",
				},
				ParamKeys: []string{"value1"},
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
				},
				PrimaryKeys: []string{"column1"},
			},
			wantErr: false,
		},
		{
			name: "query with no keyspace",
			args: args{
				query: "delete from test_table where column1 IN ('test', 'test2')",
			},
			want:    &DeleteQueryMap{},
			wantErr: true,
		},
		{
			name: "query with no table name",
			args: args{
				query: "delete from key_space. where column1 IN ('test', 'test2')",
			},
			want:    &DeleteQueryMap{},
			wantErr: true,
		},
		{
			name: "error when query is blank",
			args: args{
				query: "",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "error when timestamp is incorrect",
			args: args{
				query: "DELETE from table USING TIMESTAMP 'ABCD' where column1 = 'test1'",
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
			got, err := tr.ToSpannerDelete(tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Translator.ToSpannerDelete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil && !reflect.DeepEqual(got.SpannerQuery, tt.want.SpannerQuery) {
				t.Errorf("Translator.ToSpannerDelete() = %v, want %v", got.SpannerQuery, tt.want.SpannerQuery)
			}

			if got != nil && len(got.Params) > 0 && !reflect.DeepEqual(got.Params, tt.want.Params) {
				t.Errorf("Translator.ToSpannerDelete() = %v, want %v", got.Params, tt.want.Params)
			}

			if got != nil && len(got.ParamKeys) > 0 && !reflect.DeepEqual(got.ParamKeys, tt.want.ParamKeys) {
				t.Errorf("Translator.ToSpannerDelete() = %v, want %v", got.ParamKeys, tt.want.ParamKeys)
			}

			if got != nil && len(got.Clauses) > 0 && !reflect.DeepEqual(got.Clauses, tt.want.Clauses) {
				t.Errorf("Translator.ToSpannerDelete() = %v, want %v", got.Clauses, tt.want.Clauses)
			}

			if got != nil && !reflect.DeepEqual(got.PrimaryKeys, tt.want.PrimaryKeys) {
				t.Errorf("Translator.ToSpannerDelete() = %v, want %v", got.PrimaryKeys, tt.want.PrimaryKeys)
			}

			if got != nil && !reflect.DeepEqual(got.Table, tt.want.Table) {
				t.Errorf("Translator.ToSpannerDelete() = %v, want %v", got.Table, tt.want.Table)
			}

			if got != nil && !reflect.DeepEqual(got.Keyspace, tt.want.Keyspace) {
				t.Errorf("Translator.ToSpannerDelete() = %v, want %v", got.Keyspace, tt.want.Keyspace)
			}
		})
	}
}

func Test_createSpannerQuery(t *testing.T) {
	tests := []struct {
		name string
		args *DeleteQueryMap
		want string
	}{
		{
			name: "sucess",
			args: &DeleteQueryMap{
				Table: "testTable",
				Clauses: []Clause{
					{
						Column:   "test",
						Operator: "=",
						Value:    "1",
					},
				},
			},
			want: "DELETE FROM testTable WHERE `test` = 1;",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := createSpannerDeleteQuery(tt.args.Table, tt.args.Clauses)
			if got != "" && got != tt.want {
				t.Errorf("createSpannerQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
