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
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	cql "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/translator/cqlparser"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"go.uber.org/zap"
)

var mockTableConfig = map[string]map[string]*tableConfig.Column{
	"test_table": {
		"column1": &tableConfig.Column{
			Name:         "column1",
			CQLType:      "text",
			IsPrimaryKey: true,
			PkPrecedence: 1,
			SpannerType:  "string",
		},
		"column2": &tableConfig.Column{
			Name:        "column2",
			CQLType:     "blob",
			SpannerType: "bytes",
		},
		"column3": &tableConfig.Column{
			Name:        "column3",
			CQLType:     "boolean",
			SpannerType: "bool",
		},
		"column4": &tableConfig.Column{
			Name:        "column4",
			CQLType:     "list<text>",
			SpannerType: "array[string]",
		},
		"column5": &tableConfig.Column{
			Name:        "column5",
			CQLType:     "timestamp",
			SpannerType: "timestamp",
		},
		"column6": &tableConfig.Column{
			Name:        "column6",
			CQLType:     "int",
			SpannerType: "int64",
		},
		"column7": &tableConfig.Column{
			Name:        "column7",
			CQLType:     "frozen<set<text>>",
			SpannerType: "array[string]",
		},
		"column8": &tableConfig.Column{
			Name:        "column8",
			CQLType:     "map<text, boolean>",
			SpannerType: "array[string, bool]",
		},
		"column9": &tableConfig.Column{
			Name:        "column9",
			CQLType:     "bigint",
			SpannerType: "int64",
		},
		"column10": &tableConfig.Column{
			Name:        "column10",
			CQLType:     "float",
			SpannerType: "float64",
		},
		"column11": &tableConfig.Column{
			Name:        "column11",
			CQLType:     "map<text, text>",
			SpannerType: "array[string, string]",
		},
		"test_table": &tableConfig.Column{
			Name:         "test_table",
			CQLType:      "text",
			IsPrimaryKey: true,
			PkPrecedence: 1,
			SpannerType:  "string",
		},
	},
}

var mockPkMetadata = map[string][]tableConfig.Column{
	"test_table": {
		{
			Name:         "column1",
			CQLType:      "text",
			IsPrimaryKey: true,
			PkPrecedence: 1,
			SpannerType:  "string",
		},
	},
}

func TestTranslator_ToSpannerSelect(t *testing.T) {
	type fields struct {
		Logger *zap.Logger
	}
	type args struct {
		query    string
		keyspace string
	}

	inputRawQuery := `select column1 AS column_str, column2, column3 from  key_space.test_table
	where column1 = 'test' AND column2 < '0x0000000000000003' AND column3='true'
	AND column4='["The Parx Casino Philly Cycling Classic"]' AND column5 <= '2015-05-03 13:30:54.234' AND column6 >= '123'
	AND column7='{ "Rabobank-Liv Woman Cycling Team","Rabobank-Liv Giant","Rabobank Women Team","Nederland bloeit" }' AND column8='{i3=true, 5023.0=true}'
	AND column9 > '-10000000' AND column10 = '1.12' AND column11 = '{key1=abc, key2=abc}' ORDER BY column1 desc LIMIT 20000;`

	inputPreparedQuery := `select column1 AS column_str, column2, column3 from  key_space.test_table
	where column1 = ? AND column2 = ? AND column3 = ?
	AND column4 = ? AND column5 = ? AND column6 = ?
	AND column7 = ? AND column8 = ?
	AND column9 = ? LIMIT ?;`
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *SelectQueryMap
		wantErr bool
	}{
		{
			name: "test for raw query success",
			args: args{
				query: inputRawQuery,
			},
			want: &SelectQueryMap{
				CassandraQuery: inputRawQuery,
				QueryType:      "select",
				SpannerQuery:   "SELECT `column1` as column_str,`column2`,`column3` FROM test_table WHERE `column1` = @value1 AND `column2` < @value2 AND `column3` = @value3 AND `column4` = @value4 AND `column5` <= @value5 AND `column6` >= @value6 AND `column7` = @value7 AND `column8` = @value8 AND `column9` > @value9 AND `column10` = @value10 AND `column11` = @value11 ORDER BY `column1` desc LIMIT 20000;",
				Table:          "test_table",
				Keyspace:       "key_space",
				Clauses: []Clause{
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
				Limit: Limit{
					isLimit: true,
					Count:   "2000",
				},
				OrderBy: OrderBy{
					isOrderBy: true,
					Column:    "`column1`",
					Operation: "desc",
				},
				Params: map[string]interface{}{
					"value1": "test",
					"value2": []byte(string("0x0000000000000003")),
					"value3": true,
					"value4": []string{"The Parx Casino Philly Cycling Classic"},
					"value5": "2015-05-03 13:30:54.234",
					"value6": 123,
					"value7": []string{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"},
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
				ParamKeys: []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10", "value11"},
			},
			wantErr: false,
		},
		{
			name: "test for prepared query success",
			args: args{
				query: inputPreparedQuery,
			},
			want: &SelectQueryMap{
				CassandraQuery: inputPreparedQuery,
				QueryType:      "select",
				SpannerQuery:   "SELECT `column1` as column_str,`column2`,`column3` FROM test_table WHERE `column1` = @value1 AND `column2` = @value2 AND `column3` = @value3 AND `column4` = @value4 AND `column5` = @value5 AND `column6` = @value6 AND `column7` = @value7 AND `column8` = @value8 AND `column9` = @value9 LIMIT @limitValue;",
				Table:          "test_table",
				Keyspace:       "key_space",
				Clauses: []Clause{
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
				Limit: Limit{
					isLimit: true,
					Count:   "2000",
				},
				OrderBy: OrderBy{
					isOrderBy: false,
				},
				ParamKeys: []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "limitValue"},
			},
			wantErr: false,
		},
		{
			name: "test for query without clause success",
			args: args{
				query: `select column1, column2, column3 from key_space.test_table LIMIT 20000;`,
			},
			want: &SelectQueryMap{
				CassandraQuery: `select column1, column2, column3 from key_space.test_table LIMIT 20000;`,
				QueryType:      "select",
				SpannerQuery:   "SELECT `column1`,`column2`,`column3` FROM test_table LIMIT 20000;",
				Table:          "test_table",
				Keyspace:       "key_space",
				ColumnMeta: ColumnMeta{
					Star:   false,
					Column: []tableConfig.SelectedColumns{{Name: "column1"}, {Name: "column2"}, {Name: "column3"}},
				},
				Limit: Limit{
					isLimit: true,
					Count:   "2000",
				},
			},
			wantErr: false,
		},
		{
			name: "test for query without keyspace in query",
			args: args{
				query:    `select column1, column2, column3 from test_table;`,
				keyspace: `key_space`,
			},
			want: &SelectQueryMap{
				CassandraQuery: `select column1, column2, column3 from test_table;`,
				QueryType:      "select",
				SpannerQuery:   "SELECT `column1`,`column2`,`column3` FROM test_table;",
				Table:          "test_table",
				Keyspace:       "key_space",
				ColumnMeta: ColumnMeta{
					Star:   false,
					Column: []tableConfig.SelectedColumns{{Name: "column1"}, {Name: "column2"}, {Name: "column3"}},
				},
			},
			wantErr: false,
		},
		{
			name: "error at Column parsing",
			args: args{
				query: "select  from test_table;",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error when no keyspace provided",
			args: args{
				query: "select * from test_table where name=test;",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error at Clause parsing when column type not found",
			args: args{
				query: "select * from key_space.test_table where name=test;",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error at Clause parsing when value invalid",
			args: args{
				query: "select * from key_space.test_table where column1=",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "test with IN operator raw query",
			args: args{
				query: `select column1, column2 from key_space.test_table where column1 = 'test' and column1 in ('abc', 'xyz') AND column2 IN ('0x0000000000000003') AND column3 IN ('true')
				AND column4 IN ('["The Parx Casino Philly Cycling Classic"]') AND column5 IN ('2015-05-03 13:30:54.234') AND column6 IN ('123')
				AND column7 IN ('{ "Rabobank-Liv Woman Cycling Team","Rabobank-Liv Giant","Rabobank Women Team","Nederland bloeit" }') AND column8 IN ('{i3=true, 5023.0=true}')
				AND column9 IN ('-10000000') AND column10 IN ('1.12') AND column11 IN ('{key1=abc, key2=abc}');`,
			},
			wantErr: false,
			want: &SelectQueryMap{
				SpannerQuery: "SELECT `column1`,`column2` FROM test_table WHERE `column1` = @value1 AND `column1` IN UNNEST(@value2) AND `column2` IN UNNEST(@value3) AND `column3` IN UNNEST(@value4) AND `column4` IN UNNEST(@value5) AND `column5` IN UNNEST(@value6) AND `column6` IN UNNEST(@value7) AND `column7` IN UNNEST(@value8) AND `column8` IN UNNEST(@value9) AND `column9` IN UNNEST(@value10) AND `column10` IN UNNEST(@value11) AND `column11` IN UNNEST(@value12);",
				Keyspace:     "key_space",
				Params: map[string]interface{}{
					"value1": "test",
					"value2": []string{"abc", "xyz"},
					"value3": [][]byte{[]byte(string("0x0000000000000003"))},
					"value4": []bool{true},
					"value5": [][]string{{"The Parx Casino Philly Cycling Classic"}},
					"value6": []string{"2015-05-03 13:30:54.234"},
					"value7": []int{123},
					"value8": [][]string{{"Rabobank-Liv Woman Cycling Team", "Rabobank-Liv Giant", "Rabobank Women Team", "Nederland bloeit"}},
					"value9": []spanner.NullJSON{{Value: map[string]interface{}{
						"i3":     true,
						"5023.0": true,
					}, Valid: true}},
					"value10": []int{-10000000},
					"value11": []float64{1.12},
					"value12": []spanner.NullJSON{{Value: map[string]interface{}{
						"key1": "abc",
						"key2": "abc",
					}, Valid: true}},
				},
				ParamKeys: []string{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10", "value11", "value12"},
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
					{
						Column:       "column1",
						Operator:     "IN",
						Value:        "@value2",
						IsPrimaryKey: true,
					},
					{
						Column:   "column2",
						Operator: "IN",
						Value:    "@value3",
					},
					{
						Column:   "column3",
						Operator: "IN",
						Value:    "@value4",
					},
					{
						Column:   "column4",
						Operator: "IN",
						Value:    "@value5",
					},
					{
						Column:   "column5",
						Operator: "IN",
						Value:    "@value6",
					},
					{
						Column:   "column6",
						Operator: "IN",
						Value:    "@value7",
					},
					{
						Column:   "column7",
						Operator: "IN",
						Value:    "@value8",
					},
					{
						Column:   "column8",
						Operator: "IN",
						Value:    "@value9",
					},
					{
						Column:   "column9",
						Operator: "IN",
						Value:    "@value10",
					},
					{
						Column:   "column10",
						Operator: "IN",
						Value:    "@value11",
					},
					{
						Column:   "column11",
						Operator: "IN",
						Value:    "@value12",
					},
				},
			},
		},
		{
			name: "test with IN operator prepared query",
			args: args{
				query: `select column1, column2 from key_space.test_table where column1 = ? and column1 in ?;`,
			},
			wantErr: false,
			want: &SelectQueryMap{
				SpannerQuery: "SELECT `column1`,`column2` FROM test_table WHERE `column1` = @value1 AND `column1` IN UNNEST(@value2);",
				Keyspace:     "key_space",
				Params:       map[string]interface{}{},
				ParamKeys:    []string{"value1", "value2"},
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
					{
						Column:       "column1",
						Operator:     "IN",
						Value:        "@value2",
						IsPrimaryKey: true,
					},
				},
			},
		},
		{
			name: "test with COUNT AND ROUND function",
			args: args{
				query: `select count(column2), round(column2), round(column2) AS column2 from key_space.test_table;`,
			},
			wantErr: false,
			want: &SelectQueryMap{
				SpannerQuery: "SELECT count(`column2`) as count_column2,round(`column2`) as round_column2,round(`column2`) as column2 FROM test_table;",
				Keyspace:     "key_space",
				Params:       map[string]interface{}{},
				Clauses:      []Clause{},
			},
		},
		{
			name: "error when AS keyword is incorrectly used",
			args: args{
				query: "select column1 AS from key_space.test_table where column1='test'",
			},
			want:    nil,
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
				UseRowTimestamp: false,
			}
			got, err := tr.ToSpannerSelect(tt.args.keyspace, tt.args.query, false)
			if (err != nil) != tt.wantErr {
				t.Errorf("Translator.ToSpannerSelect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil && !reflect.DeepEqual(got.SpannerQuery, tt.want.SpannerQuery) {
				t.Errorf("Translator.ToSpannerSelect() = %v, want %v", got.SpannerQuery, tt.want.SpannerQuery)
			}

			if got != nil && len(got.Params) > 0 && !reflect.DeepEqual(got.Params, tt.want.Params) {
				for key := range got.Params {
					if !reflect.DeepEqual(got.Params[key], tt.want.Params[key]) {
						t.Errorf("Translator.ToSpannerSelect() = %v, want %v for key %v", got.Params[key], tt.want.Params[key], key)
					}
				}
			}

			if got != nil && len(got.ParamKeys) > 0 && !reflect.DeepEqual(got.ParamKeys, tt.want.ParamKeys) {
				t.Errorf("Translator.ToSpannerSelect() = %v, want %v", got.ParamKeys, tt.want.ParamKeys)
			}

			if got != nil && len(got.Clauses) > 0 && !reflect.DeepEqual(got.Clauses, tt.want.Clauses) {
				t.Errorf("Translator.ToSpannerSelect() = %v, want %v", got.Clauses, tt.want.Clauses)
			}

			if got != nil && len(got.Keyspace) > 0 && !reflect.DeepEqual(got.Keyspace, tt.want.Keyspace) {
				t.Errorf("Translator.ToSpannerSelect() = %v, want %v", got.Keyspace, tt.want.Keyspace)
			}
		})
	}
}

func Test_getSpannerSelectQuery(t *testing.T) {
	tableConfig := &tableConfig.TableConfig{
		Logger:          zap.NewNop(),
		TablesMetaData:  mockTableConfig,
		PkMetadataCache: mockPkMetadata,
	}
	tr := &Translator{
		Logger:          zap.NewNop(),
		TableConfig:     tableConfig,
		UseRowTimestamp: true,
		UseRowTTL:       true,
	}

	type args struct {
		data *SelectQueryMap
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				data: &SelectQueryMap{
					QueryType: "select",
					Table:     "test_table",
					ColumnMeta: ColumnMeta{
						Star: true,
					},
					Limit: Limit{
						isLimit: true,
						Count:   "1000",
					},
					OrderBy: OrderBy{
						isOrderBy: true,
						Column:    "`column1`",
						Operation: "desc",
					},
				},
			},
			want:    "SELECT * EXCEPT (`last_commit_ts`, `spanner_ttl_ts`) FROM test_table ORDER BY `column1` desc LIMIT 1000;",
			wantErr: false,
		},
		{
			name: "error",
			args: args{
				data: &SelectQueryMap{},
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getSpannerSelectQuery(tr, tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("getSpannerSelectQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getSpannerSelectQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseColumnsFromSelect(t *testing.T) {
	tests := []struct {
		name     string
		input    string // assuming cql is the package containing ISelectElementsContext
		expected ColumnMeta
		err      error
	}{
		{
			name:     "Test with STAR",
			input:    "*",
			expected: ColumnMeta{Star: true},
			err:      nil,
		},
		{
			name:     "Test with columns",
			input:    "columnName1, columnName2",
			expected: ColumnMeta{Column: []tableConfig.SelectedColumns{{Name: "columnname1"}, {Name: "columnname2"}}},
			err:      nil,
		},
		{
			name:     "Test with AS operator",
			input:    "columnA as aliasA, columnB as aliasB",
			expected: ColumnMeta{Column: []tableConfig.SelectedColumns{{Name: "columna", IsAs: true, Alias: "aliasa"}, {Name: "columnb", IsAs: true, Alias: "aliasb"}}},
			err:      nil,
		},
		{
			name:     "Test with nil input",
			input:    "",
			expected: ColumnMeta{},
			err:      errors.New("no Input parameters found for columns"),
		},
		{
			name:     "Test with invalid columns",
			input:    "column1 AS",
			expected: ColumnMeta{},
			err:      errors.New("one or more undefined fields for column name"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var input cql.ISelectElementsContext
			if test.input == "" {
				input = nil
			} else {
				p, _ := NewCqlParser(test.input, true)
				input = p.SelectElements()

			}
			result, err := parseColumnsFromSelect(input)
			if !reflect.DeepEqual(result, test.expected) {
				t.Errorf("Expected result %+v, but got %+v", test.expected, result)
			}
			if (err == nil && test.err != nil) || (err != nil && test.err == nil) || (err != nil && test.err != nil && err.Error() != test.err.Error()) {
				t.Errorf("Expected error %+v, but got %+v", test.err, err)
			}
		})
	}
}

func TestParseTableFromSelect(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *TableObj
		err      error
	}{
		{
			name:  "Valid test",
			input: "FROM key_space.test_table",
			expected: &TableObj{
				TableName:    "test_table",
				KeyspaceName: "key_space",
			},
			err: nil,
		},
		{
			name:     "Test with nil input",
			input:    "",
			expected: nil,
			err:      errors.New("no input parameters found for table and keyspace"),
		},
		{
			name:     "Test with no table name",
			input:    "FROM",
			expected: nil,
			err:      errors.New("could not find table and keyspace name"),
		},
		{
			name:     "Test with no keyspace",
			input:    "FROM table_name",
			expected: nil,
			err:      errors.New("could not find keyspace name"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var input cql.IFromSpecContext
			if test.input == "" {
				input = nil
			} else {
				p, _ := NewCqlParser(test.input, true)
				input = p.FromSpec()

			}
			result, err := parseTableFromSelect("", input)
			if !reflect.DeepEqual(result, test.expected) {
				t.Errorf("Expected result %+v, but got %+v", test.expected, result)
			}
			if (err == nil && test.err != nil) || (err != nil && test.err == nil) || (err != nil && test.err != nil && err.Error() != test.err.Error()) {
				t.Errorf("Expected error %+v, but got %+v", test.err, err)
			}
		})
	}
}

func TestProcessStrings(t *testing.T) {
	mockTranslator := &Translator{TableConfig: &tableConfig.TableConfig{
		TablesMetaData:  mockTableConfig,
		PkMetadataCache: mockPkMetadata,
	}}
	tests := []struct {
		name         string
		inputStrings []tableConfig.SelectedColumns
		tableName    string
		expected     map[string]tableConfig.AsKeywordMeta
		expectedCols []string
		err          error
	}{
		{
			name: "Test with valid input functions",
			inputStrings: []tableConfig.SelectedColumns{
				{Name: "column1", IsFunc: true, FuncName: "count", IsAs: true, Alias: "column1"},
				{Name: "column2", IsFunc: true, FuncName: "round", IsAs: true, Alias: "column2"},
			},
			tableName: "testTable",
			expected: map[string]tableConfig.AsKeywordMeta{
				"column1": {IsFunc: true, Name: "column1", Alias: "column1", CQLType: "bigint", DataType: datatype.Bigint},
				"column2": {IsFunc: true, Name: "column2", Alias: "column2", CQLType: "float", DataType: datatype.Float},
			},
			expectedCols: []string{
				"count(`column1`) as column1",
				"round(`column2`) as column2",
			},
			err: nil,
		},
		{
			name: "Test with valid input columns",
			inputStrings: []tableConfig.SelectedColumns{
				{Name: "column1", IsAs: true, Alias: "column1"},
				{Name: "last_commit_ts"},
			},
			tableName: "test_table",
			expected: map[string]tableConfig.AsKeywordMeta{
				"column1": {IsFunc: false, Name: "column1", Alias: "column1", CQLType: "text", DataType: datatype.Varchar},
			},
			expectedCols: []string{
				"`column1` as column1",
				"`last_commit_ts`",
			},
			err: nil,
		},
		{
			name: "Test with invalid input",
			inputStrings: []tableConfig.SelectedColumns{
				{Name: "invalidFunc1", IsFunc: true, FuncName: "invalidFunc1", IsAs: true, Alias: "alias1"},
			},
			tableName:    "testTable",
			expected:     nil,
			expectedCols: nil,
			err:          errors.New("Unknown function 'invalidFunc1'"),
		},
		{
			name: "Test with tableName and columnName same",
			inputStrings: []tableConfig.SelectedColumns{
				{Name: "test_table", IsFunc: true, FuncName: "count", IsAs: true, Alias: "testTable"},
				{Name: "column1", IsFunc: true, FuncName: "round", IsAs: true, Alias: "column1"},
			},
			tableName: "test_table",
			expected: map[string]tableConfig.AsKeywordMeta{
				"testTable": {IsFunc: true, Name: "test_table", Alias: "testTable", CQLType: "bigint", DataType: datatype.Bigint},
				"column1":   {IsFunc: true, Name: "column1", Alias: "column1", CQLType: "float", DataType: datatype.Float},
			},
			expectedCols: []string{
				"count(`test_table`.`test_table`) as testTable",
				"round(`column1`) as column1",
			},
			err: nil,
		},
		{name: "Test with tableName and columnName same without function",
			inputStrings: []tableConfig.SelectedColumns{
				{Name: "test_table", IsAs: true, Alias: "testTable"},
				{Name: "column1", IsAs: true, Alias: "column1"},
			},
			tableName: "test_table",
			expected: map[string]tableConfig.AsKeywordMeta{
				"column1":   {IsFunc: false, Name: "column1", Alias: "column1", CQLType: "text", DataType: datatype.Varchar},
				"testTable": {IsFunc: false, Name: "test_table", Alias: "testTable", CQLType: "text", DataType: datatype.Varchar},
			},
			expectedCols: []string{
				"`test_table`.`test_table` as testTable",
				"`column1` as column1",
			},
			err: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, columns, err := processStrings(mockTranslator, test.inputStrings, test.tableName)
			if !reflect.DeepEqual(result, test.expected) {
				t.Errorf("Expected result %+v, but got %+v", test.expected, result)
			}
			if !reflect.DeepEqual(columns, test.expectedCols) {
				t.Errorf("Expected columns %+v, but got %+v", test.expectedCols, columns)
			}
			if (err == nil && test.err != nil) || (err != nil && test.err == nil) || (err != nil && test.err != nil && err.Error() != test.err.Error()) {
				t.Errorf("Expected error %+v, but got %+v", test.err, err)
			}
		})
	}
}

func TestInferDataType(t *testing.T) {
	tests := []struct {
		name         string
		methodName   string
		expectedType string
		expectedErr  error
	}{
		{
			name:         "Test count function",
			methodName:   "count",
			expectedType: "bigint",
			expectedErr:  nil,
		},
		{
			name:         "Test round function",
			methodName:   "round",
			expectedType: "float",
			expectedErr:  nil,
		},
		{
			name:         "Test unsupported function",
			methodName:   "unsupported",
			expectedType: "",
			expectedErr:  errors.New("Unknown function 'unsupported'"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := inferDataType(test.methodName)
			if result != test.expectedType {
				t.Errorf("Expected result type %s, but got %s", test.expectedType, result)
			}
			if (err == nil && test.expectedErr != nil) || (err != nil && test.expectedErr == nil) || (err != nil && test.expectedErr != nil && err.Error() != test.expectedErr.Error()) {
				t.Errorf("Expected error %+v, but got %+v", test.expectedErr, err)
			}
		})
	}
}

func TestParseLimitFromSelect(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedLimit Limit
	}{
		{
			name:          "Valid input with limit",
			input:         "LIMIT 10",
			expectedLimit: Limit{isLimit: true, Count: "10"},
		},
		{
			name:          "Valid input with limit placeholder",
			input:         "LIMIT ?",
			expectedLimit: Limit{isLimit: true, Count: questionMark},
		},
		{
			name:          "Valid input with nil",
			input:         "",
			expectedLimit: Limit{isLimit: false},
		},
		{
			name:  "Invalid input",
			input: "LIMIT ",
			expectedLimit: Limit{
				isLimit: false,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var input cql.ILimitSpecContext
			if test.input == "" {
				input = nil
			} else {
				p, _ := NewCqlParser(test.input, true)
				input = p.LimitSpec()

			}
			result := parseLimitFromSelect(input)

			// Validate isLimit flag
			if result.isLimit != test.expectedLimit.isLimit {
				t.Errorf("Expected isLimit %v, but got %v", test.expectedLimit.isLimit, result.isLimit)
			}

			// Validate Count
			if result.Count != test.expectedLimit.Count {
				t.Errorf("Expected Count %s, but got %s", test.expectedLimit.Count, result.Count)
			}
		})
	}
}

func TestParseOrderByFromSelect(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		expectedOrderBy OrderBy
	}{
		{
			name:  "Valid input with asc",
			input: "ORDER BY columnName ASC",
			expectedOrderBy: OrderBy{
				isOrderBy: true,
				Column:    "`columnName`",
				Operation: "asc",
			},
		},
		{
			name:  "Valid input with desc",
			input: "ORDER BY columnName DESC",
			expectedOrderBy: OrderBy{
				isOrderBy: true,
				Column:    "`columnName`",
				Operation: "desc",
			},
		},
		{
			name:  "Valid input with nil",
			input: "",
			expectedOrderBy: OrderBy{
				isOrderBy: false,
			},
		},
		{
			name:  "Invalid input",
			input: "ORDER BY",
			expectedOrderBy: OrderBy{
				isOrderBy: false,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var input cql.IOrderSpecContext
			if test.input == "" {
				input = nil
			} else {
				p, _ := NewCqlParser(test.input, true)
				input = p.OrderSpec()

			}
			result := parseOrderByFromSelect(input)

			// Validate isOrderBy flag
			if result.isOrderBy != test.expectedOrderBy.isOrderBy {
				t.Errorf("Expected isOrderBy %v, but got %v", test.expectedOrderBy.isOrderBy, result.isOrderBy)
			}

			// Validate Column
			if result.Column != test.expectedOrderBy.Column {
				t.Errorf("Expected Column %s, but got %s", test.expectedOrderBy.Column, result.Column)
			}

			// Validate Operation
			if result.Operation != test.expectedOrderBy.Operation {
				t.Errorf("Expected Operation %s, but got %s", test.expectedOrderBy.Operation, result.Operation)
			}
		})
	}
}
