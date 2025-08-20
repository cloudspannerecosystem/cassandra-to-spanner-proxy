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
	"fmt"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	cql "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/translator/cqlparser"
)

func Test_convertMapString(t *testing.T) {
	type args struct {
		dataStr string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "sucess",
			args: args{
				dataStr: "{i3=true, 5023.0=true}",
			},
			want: map[string]interface{}{
				"i3":     true,
				"5023.0": true,
			},
			wantErr: false,
		},
		{
			name: "error",
			args: args{
				dataStr: "test",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertMapString(tt.args.dataStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertMapString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertMapString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hasWhere(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "success",
			args: args{
				query: "select * from table where id = 1;",
			},
			want: true,
		},
		{
			name: "no where clause",
			args: args{
				query: "select * from table;",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasWhere(tt.args.query); got != tt.want {
				t.Errorf("hasWhere() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hasOrderBy(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "has order by",
			query: "SELECT * FROM users ORDER BY id",
			want:  true,
		},
		{
			name:  "no order by",
			query: "SELECT * FROM users",
			want:  false,
		},
		{
			name:  "has order by with multiple columns",
			query: "SELECT * FROM users ORDER BY id, name",
			want:  true,
		},
		{
			name:  "has order by with descending order",
			query: "SELECT * FROM users ORDER BY id DESC",
			want:  true,
		},
		{
			name:  "has order by with ascending order",
			query: "SELECT * FROM users ORDER BY id ASC",
			want:  true,
		},
		{
			name:  "has order by with whitespace",
			query: "SELECT * FROM users ORDER BY  id ",
			want:  true,
		},
		{
			name:  "has order by with lowercase",
			query: "select * from users order by id",
			want:  true,
		},
		{
			name:  "has order by with uppercase",
			query: "SELECT * FROM users ORDER BY ID",
			want:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasOrderBy(tt.query); got != tt.want {
				t.Errorf("hasOrderBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hasLimit(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "Query with limit",
			query: "SELECT * FROM table LIMIT 10",
			want:  true,
		},
		{
			name:  "Query with lowecase limit",
			query: "SELECT * FROM table limit 10",
			want:  true,
		},
		{
			name:  "Query without limit",
			query: "SELECT * FROM table",
			want:  false,
		},
		{
			name:  "Empty query",
			query: "",
			want:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasLimit(tt.query); got != tt.want {
				t.Errorf("hasLimit() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hasUsingTimestamp(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "has using timestamp with insert",
			query: "insert into test (key, value) values ('mykey', 'somevalue') using timestamp 123456789",
			want:  true,
		},
		{
			name:  "no using timestamp with update",
			query: "update key_space.test_table using timestamp '?' and ttl '?' set column1 = '?' , column2 = '?'",
			want:  true,
		},
		{
			name:  "using timestamp in uppercase",
			query: "INSERT INTO test (key, value) VALUES ('mykey', 'somevalue') USING TIMESTAMP 123456789",
			want:  false,
		},
		{
			name:  "no using timestamp",
			query: "INSERT INTO test (key, value) VALUES ('mykey', 'somevalue')",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasUsingTimestamp(tt.query); got != tt.want {
				t.Errorf("hasUsingTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_formatValues(t *testing.T) {

	ts1, _ := parseTimestamp("2022-01-01T00:00:00Z")
	ts2, _ := parseTimestamp("2022-01-02T00:00:00Z")

	testCases := []struct {
		name        string
		value       string
		spannerType string
		cqlType     string
		expected    interface{}
		expectedErr error
	}{
		{
			name:        "String type",
			value:       "hello",
			spannerType: "string",
			cqlType:     "",
			expected:    "hello",
			expectedErr: nil,
		},
		{
			name:        "String type with null value",
			value:       "null",
			spannerType: "string",
			cqlType:     "",
			expected:    nil,
			expectedErr: nil,
		},
		{
			name:        "Bytes type",
			value:       "hello",
			spannerType: "bytes",
			cqlType:     "",
			expected:    []byte("hello"),
			expectedErr: nil,
		},
		{
			name:        "Timestamp type",
			value:       "2022-01-01T00:00:00Z",
			spannerType: "timestamp",
			cqlType:     "",
			expected:    "2022-01-01T00:00:00Z",
			expectedErr: nil,
		},
		{
			name:        "Int64 type",
			value:       "123",
			spannerType: "int64",
			cqlType:     "",
			expected:    123,
			expectedErr: nil,
		},
		{
			name:        "Int64 type with null value",
			value:       "NULL",
			spannerType: "int64",
			cqlType:     "",
			expected:    nil,
			expectedErr: nil,
		},
		{
			name:        "Timestamp type with null value",
			value:       "null",
			spannerType: "TIMESTAMP",
			cqlType:     "",
			expected:    nil,
			expectedErr: nil,
		},
		{
			name:        "Float64 type",
			value:       "3.14",
			spannerType: "float64",
			cqlType:     "",
			expected:    float64(3.14),
			expectedErr: nil,
		},
		{
			name:        "Bool type",
			value:       "true",
			spannerType: "bool",
			cqlType:     "",
			expected:    true,
			expectedErr: nil,
		},
		{
			name:        "Array[string] type with set<text> CQL type",
			value:       `["value1", "value2", "value1"]`,
			spannerType: "array[string]",
			cqlType:     "set<text>",
			expected:    []string{"value1", "value2"},
			expectedErr: nil,
		},
		{
			name:        "Array[string, bool] type",
			value:       `{"key1": true, "key2": false}`,
			spannerType: "array[string, bool]",
			cqlType:     "",
			expected: spanner.NullJSON{
				Value: map[string]interface{}{
					"key1": true,
					"key2": false,
				},
				Valid: true,
			},
			expectedErr: nil,
		},
		{
			name:        "Array[string, timestamp] type",
			value:       `{"key1": "2022-01-01T00:00:00Z", "key2": "2022-01-02T00:00:00Z"}`,
			spannerType: "array[string, timestamp]",
			cqlType:     "map<text, timestamp>",
			expected: spanner.NullJSON{
				Value: map[string]interface{}{
					"key1": ts1,
					"key2": ts2,
				},
				Valid: true,
			},
			expectedErr: nil,
		},
		{
			name:        "Array[string, string] type",
			value:       `{"key1": "value1", "key2": "value2"}`,
			spannerType: "array[string, string]",
			cqlType:     "map<text, text>",
			expected: spanner.NullJSON{
				Value: map[string]interface{}{
					"key1": "value1",
					"key2": "value2",
				},
				Valid: true,
			},
			expectedErr: nil,
		},
		{
			name:        "Unknown type",
			value:       "unknown",
			spannerType: "unknown",
			cqlType:     "",
			expected:    nil,
			expectedErr: errors.New("no correct Datatype found for column"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := formatValues(tc.value, tc.spannerType, tc.cqlType)

			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("Failed %s. Expected %v, got %v.", tc.name, tc.expected, result)
			}

			if tc.expectedErr != nil && tc.expectedErr.Error() != err.Error() {
				t.Errorf("Failed %s. Expected error: %v, got error: %v.", tc.name, tc.expectedErr, err)
			}
		})
	}
}

// TestParseTimestamp tests the parseTimestamp function with various timestamp formats.
func TestParseTimestamp(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected time.Time
		wantErr  bool
	}{
		{
			name:     "ISO 8601 format",
			input:    "2024-02-05T14:00:00Z",
			expected: time.Date(2024, 2, 5, 14, 0, 0, 0, time.UTC),
		},
		{
			name:     "Common date-time format",
			input:    "2024-02-05 14:00:00",
			expected: time.Date(2024, 2, 5, 14, 0, 0, 0, time.UTC),
		},
		{
			name:     "Unix timestamp (seconds)",
			input:    "1672522562",
			expected: time.Unix(1672522562, 0),
		},
		{
			name:     "Unix timestamp (milliseconds)",
			input:    "1672522562000",
			expected: time.Unix(0, 1672522562000*int64(time.Millisecond)),
		},
		{
			name:     "Unix timestamp (microseconds)",
			input:    "1672522562000000",
			expected: time.Unix(0, 1672522562000000*int64(time.Microsecond)),
		},
		{
			name:    "Invalid format",
			input:   "invalid-timestamp",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTimestamp(tc.input)
			if (err != nil) != tc.wantErr {
				t.Errorf("parseTimestamp() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			// Allow a small margin of error for floating-point timestamp comparisons
			if !tc.wantErr {
				delta := got.Sub(tc.expected)
				if delta > time.Millisecond || delta < -time.Millisecond {
					t.Errorf("parseTimestamp() = %v, want %v (delta: %v)", got, tc.expected, delta)
				}
			}
		})
	}
}

// TestConvertMap tests the convertMap function with various inputs.
func TestConvertToMap(t *testing.T) {
	cases := []struct {
		name     string
		dataStr  string
		cqlType  string
		expected map[string]interface{}
		wantErr  bool
	}{
		{
			name:    "Valid map<text, timestamp>",
			dataStr: `{"key1":"2024-02-05T14:00:00Z", "key2":"2024-02-05T14:00:00Z"}`,
			cqlType: "map<text, timestamp>",
			expected: map[string]interface{}{
				"key1": time.Date(2024, 2, 5, 14, 0, 0, 0, time.UTC),
				"key2": time.Date(2024, 2, 5, 14, 0, 0, 0, time.UTC),
			},
		},
		{
			name:    "Valid map<text, timestamp> Case 2",
			dataStr: `{"key1":"2023-01-30T09:00:00Z"}`,
			cqlType: "map<text, timestamp>",
			expected: map[string]interface{}{
				"key1": time.Date(2023, time.January, 30, 9, 0, 0, 0, time.UTC),
			},
		},
		{
			name:    "Valid map<text, timestamp> time in unix seconds",
			dataStr: `{"key1":1675075200}`,
			cqlType: "map<text, timestamp>",
			expected: map[string]interface{}{
				"key1": time.Date(2023, time.January, 30, 10, 40, 0, 0, time.UTC),
			},
		},
		{
			name:    "Valid map<text, timestamp> time in unix miliseconds",
			dataStr: `{"key1":1675075200}`,
			cqlType: "map<text, timestamp>",
			expected: map[string]interface{}{
				"key1": time.Date(2023, time.January, 30, 10, 40, 0, 0, time.UTC),
			},
		},
		{
			name:    "Valid map<text, timestamp> time in unix miliseconds and equal operator",
			dataStr: `{"key1"=1675075200}`,
			cqlType: "map<text, timestamp>",
			expected: map[string]interface{}{
				"key1": time.Date(2023, time.January, 30, 10, 40, 0, 0, time.UTC),
			},
		},
		{
			name:    "Valid map<text, text> with equal ",
			dataStr: `{"key1"="1675075200"}`,
			cqlType: "map<text, text>",
			expected: map[string]interface{}{
				"key1": "1675075200",
			},
		},
		{
			name:    "Valid map<text, text> with colon",
			dataStr: `{"key1":"1675075200"}`,
			cqlType: "map<text, text>",
			expected: map[string]interface{}{
				"key1": "1675075200",
			},
		},
		{
			name:    "Invalid pair formate",
			dataStr: `{"key11675075200, "sdfdgdgdf"}`,
			cqlType: "map<text, text>",
			wantErr: true,
		},
		{
			name:    "Invalid timestamp format",
			dataStr: `{"key1": "invalid-timestamp"}`,
			cqlType: "map<text, timestamp>",
			wantErr: true,
		},
		// Add more test cases as needed
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertToMap(tc.dataStr, tc.cqlType)
			if (err != nil) != tc.wantErr {
				t.Errorf("convertMap() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tc.expected) && !tc.wantErr {
				t.Errorf("convertMap() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestMapOfStringToString(t *testing.T) {
	cases := []struct {
		name     string
		pairs    []string
		expected map[string]interface{}
		wantErr  bool
	}{
		{
			name:  "Valid pairs with colon separator",
			pairs: []string{"key1:value1", "key2:value2"},
			expected: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
			wantErr: false,
		},
		{
			name:  "Valid pairs with equal separator",
			pairs: []string{"key1=value1", "key2=value2"},
			expected: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
			wantErr: false,
		},
		{
			name:    "Invalid pair format",
			pairs:   []string{"key1-value1"},
			wantErr: true,
		},
		{
			name:  "Mixed separators",
			pairs: []string{"key1:value1", "key2=value2"},
			expected: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
			wantErr: false,
		},
		// Add more test cases as needed
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mapOfStringToString(tc.pairs)
			if (err != nil) != tc.wantErr {
				t.Errorf("mapOfStringToString() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tc.expected) && !tc.wantErr {
				t.Errorf("mapOfStringToString() got = %v, want %v", got, tc.expected)
			}
		})
	}
}

// TestReplaceWriteTimePatterns tests the replaceWriteTimePatterns function to ensure it correctly identifies
// and replaces 'writetime(column_name)' and 'writetime(column_name) as alias' patterns in SQL queries.
func TestReplaceWriteTimePatterns(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "Single writetime without alias",
			query:    "SELECT writetime(column1) FROM myTable",
			expected: "SELECT last_commit_ts as writetime_column1 FROM myTable",
		},
		{
			name:     "Single writetime with alias",
			query:    "SELECT writetime(column2) as aliasName FROM myTable",
			expected: "SELECT last_commit_ts as aliasName FROM myTable",
		},
		{
			name:     "Multiple writetime, mixed alias",
			query:    "SELECT column1, writetime(column2), writetime(column3) as columnAlias FROM myTable",
			expected: "SELECT column1, last_commit_ts as writetime_column2, last_commit_ts as columnAlias FROM myTable",
		},
		{
			name:     "No writetime function",
			query:    "SELECT column1 FROM myTable",
			expected: "SELECT column1 FROM myTable",
		},
		{
			name:     "Case Insensitive Check",
			query:    "SELECT Writetime(column4) As AliasTwo FROM myTable",
			expected: "SELECT last_commit_ts as AliasTwo FROM myTable",
		},

		{
			name:     "multiple column with as",
			query:    "SELECT Writetime(column4) As AliasTwo, count(col3) as count_col3 FROM myTable",
			expected: "SELECT last_commit_ts as AliasTwo, count(col3) as count_col3 FROM myTable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := replaceWriteTimePatterns(tc.query)
			if result != tc.expected {
				t.Errorf("Failed %s. Expected %q, got %q", tc.name, tc.expected, result)
			}
		})
	}
}

func TestGetUniqueSet(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "No duplicates",
			input:    []string{"a", "b", "c"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "Duplicates present",
			input:    []string{"a", "b", "c", "b", "a"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "Empty input",
			input:    []string{},
			expected: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := getUniqueSet(test.input)

			if !reflect.DeepEqual(result, test.expected) {
				t.Errorf("Expected %v, got %v", test.expected, result)
			}
		})
	}
}

func TestBuildWhereClause(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name     string
		clauses  []Clause
		expected string
	}{
		{
			name:     "Empty clauses",
			clauses:  []Clause{},
			expected: "",
		},
		{
			name: "Single clause",
			clauses: []Clause{
				{
					Column:   "column1",
					Operator: "=",
					Value:    "1",
				},
			},
			expected: " WHERE `column1` = 1",
		},
		{
			name: "Multiple clauses",
			clauses: []Clause{
				{
					Column:   "column1",
					Operator: "=",
					Value:    "value1",
				},
				{
					Column:   "column2",
					Operator: ">",
					Value:    "value2",
				},
				{
					Column:   "column3",
					Operator: "IN",
					Value:    "value3",
				},
			},
			expected: " WHERE `column1` = value1 AND `column2` > value2 AND `column3` IN UNNEST(value3)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildWhereClause(tc.clauses)
			if result != tc.expected {
				t.Errorf("Failed %s. Expected %s, got %s.", tc.name, tc.expected, result)
			}
		})
	}
}

func TestNewCqlParser(t *testing.T) {
	tests := []struct {
		name        string
		cqlQuery    string
		expectedErr error
	}{
		{
			name:        "Valid input",
			cqlQuery:    "SELECT * FROM table;",
			expectedErr: nil,
		},
		{
			name:        "Empty input",
			cqlQuery:    "",
			expectedErr: fmt.Errorf("invalid input string"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Call the NewCqlParser function
			parser, err := NewCqlParser(test.cqlQuery, true)

			// Check if the error matches the expected error
			if !errorEquals(err, test.expectedErr) {
				t.Errorf("Expected error: %v, got: %v", test.expectedErr, err)
			}

			// Check if the parser matches the expected parser
			if err == nil && parser == nil {
				t.Errorf("Expected parser: not nil, got: %v", parser)
			}
		})
	}
}

// Helper function to compare errors
func errorEquals(err1, err2 error) bool {
	if (err1 == nil && err2 != nil) || (err1 != nil && err2 == nil) {
		return false
	}
	if err1 != nil && err2 != nil && err1.Error() != err2.Error() {
		return false
	}
	return true
}

func TestRenameLiterals(t *testing.T) {
	tests := []struct {
		name           string
		inputQuery     string
		expectedOutput string
	}{
		{
			name:           "Replace 'time'",
			inputQuery:     "SELECT time FROM table;",
			expectedOutput: "SELECT time_a1b2c3 FROM table;",
		},
		{
			name:           "Replace 'key'",
			inputQuery:     "SELECT key FROM table WHERE type = 'test';",
			expectedOutput: "SELECT key_a1b2c3 FROM table WHERE type_a1b2c3 = 'test';",
		},
		{
			name:           "Replace 'type'",
			inputQuery:     "SELECT type FROM table WHERE json = 'data';",
			expectedOutput: "SELECT type_a1b2c3 FROM table WHERE json_a1b2c3 = 'data';",
		},
		{
			name:           "Replace 'json'",
			inputQuery:     "SELECT json FROM table WHERE type = 'example';",
			expectedOutput: "SELECT json_a1b2c3 FROM table WHERE type_a1b2c3 = 'example';",
		},
		{
			name:           "No replacements",
			inputQuery:     "SELECT column1, column2 FROM table WHERE condition = true;",
			expectedOutput: "SELECT column1, column2 FROM table WHERE condition = true;",
		},
		{
			name:           "Partial match",
			inputQuery:     "SELECT column1, column2 FROM table WHERE time_column = '2022-05-14';",
			expectedOutput: "SELECT column1, column2 FROM table WHERE time_column = '2022-05-14';",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Call the renameLiterals function
			output := renameLiterals(test.inputQuery)

			// Compare the output with the expected output
			if output != test.expectedOutput {
				t.Errorf("Expected output: %s, got: %s", test.expectedOutput, output)
			}
		})
	}
}

func TestCreateCombinedRegex(t *testing.T) {
	regex := createCombinedRegex()

	tests := []struct {
		input    string
		expected bool
	}{
		{"time", true},
		{"key", true},
		{"type", true},
		{"json", true},
		{"Time", false}, // case-sensitive check
		{"Key", false},  // case-sensitive check
		{"Type", false}, // case-sensitive check
		{"Json", false}, // case-sensitive check
		{"timestamp", false},
		{"keynote", false},
		{"typename", false},
		{"jsonify", false},
		{"other", false},
	}

	for _, test := range tests {
		result := regex.MatchString(test.input)
		if result != test.expected {
			t.Errorf("For input '%s', expected %v, but got %v", test.input, test.expected, result)
		}
	}
}

func TestParseWhereByClause(t *testing.T) {

	tests := []struct {
		name           string
		input          string
		tableName      string
		tableConfig    tableConfig.TableConfig
		isQuery        bool
		keyspace       string
		expectedResult *ClauseResponse
		expectedErr    error
	}{
		{
			name:      "Valid input with one clause",
			input:     "WHERE column1 = 'test'",
			tableName: "test_table",
			keyspace:  "key_space",
			expectedResult: &ClauseResponse{
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
				},
				Params: map[string]interface{}{
					"value1": "test",
				},
				ParamKeys: []string{"value1"},
			},
			expectedErr: nil,
		},
		{
			name:      "Valid input with question mark",
			input:     "WHERE column1 = '?'",
			tableName: "test_table",
			keyspace:  "key_space",
			expectedResult: &ClauseResponse{
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
				},
				Params: map[string]interface{}{
					"value1": "?",
				},
				ParamKeys: []string{"value1"},
			},
			expectedErr: nil,
		},
		{
			name:      "Valid input with multiple clauses",
			input:     "WHERE column1 = 'test' AND column2 < '0x0000000000000003'",
			tableName: "test_table",
			keyspace:  "key_space",
			expectedResult: &ClauseResponse{
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "=",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
					{
						Column:       "column2",
						Operator:     "<",
						Value:        "@value2",
						IsPrimaryKey: false,
					},
				},
				Params: map[string]interface{}{
					"value1": "test",
					"value2": []byte(string("0x0000000000000003")),
				},
				ParamKeys: []string{"value1", "value2"},
			},
			expectedErr: nil,
		},
		{
			name:      "Valid input with IN operator",
			input:     "column1 IN ('test1', 'test2')",
			tableName: "test_table",
			keyspace:  "key_space",

			expectedResult: &ClauseResponse{
				Clauses: []Clause{
					{
						Column:       "column1",
						Operator:     "IN",
						Value:        "@value1",
						IsPrimaryKey: true,
					},
				},
				Params: map[string]interface{}{
					"value1": []string{"test1", "test2"},
				},
				ParamKeys: []string{"value1"},
			},
			expectedErr: nil,
		},
		{
			name:           "Valid input with unsupported operator",
			input:          "WHERE column1 + 'test1'",
			tableName:      "test_table",
			keyspace:       "key_space",
			expectedResult: nil,
			expectedErr:    errors.New("no supported operator found"),
		},
		{
			name:           "Valid input with unsupported column type",
			input:          "WHERE unsupported_column1 = 'test1'",
			tableName:      "test_table",
			keyspace:       "key_space",
			expectedResult: nil,
			expectedErr:    errors.New("could not find column(unsupported_column1) metadata. Please insert all the table and column information into the config table"),
		},
		{
			name:           "Invalid input with empty input parameter",
			input:          "",
			tableName:      "test_table",
			keyspace:       "key_space",
			expectedResult: nil,
			expectedErr:    errors.New("no input parameters found for clauses"),
		},
		{
			name:           "Invalid input with nil elements",
			input:          "WHERE",
			tableName:      "test_table",
			keyspace:       "key_space",
			expectedResult: nil,
			expectedErr:    errors.New("could not parse column object"),
		},
		{
			name:           "Invalid input with nil constant",
			input:          "WHERE column1=",
			tableName:      "test_table",
			keyspace:       "key_space",
			expectedResult: nil,
			expectedErr:    errors.New("could not parse value from query for one of the clauses"),
		},
	}

	for _, test := range tests {
		tableConfig := &tableConfig.TableConfig{
			TablesMetaData:  mockTableConfig,
			PkMetadataCache: mockPkMetadata,
		}

		t.Run(test.name, func(t *testing.T) {
			var input cql.IWhereSpecContext
			if test.input == "" {
				input = nil
			} else {
				p, _ := NewCqlParser(test.input, true)
				input = p.WhereSpec()

			}
			result, err := parseWhereByClause(input, test.tableName, tableConfig, true)

			// Validate error
			if (err == nil && test.expectedErr != nil) || (err != nil && test.expectedErr == nil) || (err != nil && test.expectedErr != nil && err.Error() != test.expectedErr.Error()) {
				t.Errorf("Expected error %v, but got %v", test.expectedErr, err)
			}

			// Validate result
			if result == nil && test.expectedResult != nil {
				t.Error("Expected non-nil result, but got nil")
			} else if result != nil && test.expectedResult == nil {
				t.Error("Expected nil result, but got non-nil")
			} else if result != nil && test.expectedResult != nil {

				if !reflect.DeepEqual(result, test.expectedResult) {
					t.Errorf("Expected result %+v, but got %+v", test.expectedResult, result)
				}
				// Validate clauses
				if len(result.Clauses) != len(test.expectedResult.Clauses) {
					t.Errorf("Expected %d clauses, but got %d", len(test.expectedResult.Clauses), len(result.Clauses))
				}

				// Validate params
				if len(result.Params) != len(test.expectedResult.Params) {
					t.Errorf("Expected %d params, but got %d", len(test.expectedResult.Params), len(result.Params))
				}
			}
		})
	}
}
