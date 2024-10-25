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

package utilities

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// TestFlattenTableName tests the FlattenTableName function with various inputs
func TestFlattenTableName(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name            string
		keyspaceFlatter bool
		keyspace        string
		tableName       string
		expected        string
	}{
		{
			name:            "With flattening",
			keyspaceFlatter: true,
			keyspace:        "MyKeyspace",
			tableName:       "MyTable",
			expected:        "mykeyspace_mytable",
		},
		{
			name:            "Without flattening",
			keyspaceFlatter: false,
			keyspace:        "MyKeyspace",
			tableName:       "MyTable",
			expected:        "mytable",
		},
		{
			name:            "Empty keyspace and table name",
			keyspaceFlatter: true,
			keyspace:        "",
			tableName:       "",
			expected:        "_",
		},
		{
			name:            "Flatten with empty keyspace",
			keyspaceFlatter: true,
			keyspace:        "",
			tableName:       "MyTable",
			expected:        "_mytable",
		},
		{
			name:            "Flatten with empty table name",
			keyspaceFlatter: true,
			keyspace:        "MyKeyspace",
			tableName:       "",
			expected:        "mykeyspace_",
		},
		{
			name:            "No flattening with empty keyspace",
			keyspaceFlatter: false,
			keyspace:        "",
			tableName:       "MyTable",
			expected:        "mytable",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Call the function with test case parameters
			result := FlattenTableName(tc.keyspaceFlatter, tc.keyspace, tc.tableName)

			// Check if the result matches the expected value
			if result != tc.expected {
				t.Errorf("Failed %s. Expected %s, got %s.", tc.name, tc.expected, result)
			}
		})
	}
}

func TestFormatTimestamp(t *testing.T) {
	tests := []struct {
		name          string
		input         int64
		expectedTime  time.Time
		expectedError error
	}{
		{
			name:          "Timestamp less than a second",
			input:         500, // Some value less than a second
			expectedTime:  time.Unix(500, 0),
			expectedError: nil,
		},
		{
			name:          "Timestamp in seconds",
			input:         1620655315, // Some value in seconds
			expectedTime:  time.Unix(1620655315, 0),
			expectedError: nil,
		},
		{
			name:          "Timestamp in milliseconds",
			input:         1620655315000, // Some value in milliseconds
			expectedTime:  time.Unix(1620655315, 0),
			expectedError: nil,
		},
		{
			name:          "Timestamp in microseconds",
			input:         1620655315000000, // Some value in microseconds
			expectedTime:  time.Unix(1620655315, 0),
			expectedError: nil,
		},
		{
			name:          "Timestamp in nanoseconds",
			input:         162065531000000000, // Some value in nanoseconds
			expectedTime:  time.Unix(162065531, 0),
			expectedError: nil,
		},
		{
			name:          "Zero timestamp",
			input:         0, // Invalid timestamp
			expectedTime:  time.Time{},
			expectedError: errors.New("no valid timestamp found"),
		},
		{
			name:          "Invalid timestamp",
			input:         2893187128318378367, // Invalid timestamp
			expectedTime:  time.Time{},
			expectedError: errors.New("no valid timestamp found"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := FormatTimestamp(test.input)

			if result != nil && !result.Equal(test.expectedTime) {
				t.Errorf("Expected time: %v, got: %v", test.expectedTime, result)
			}

			if !errorEquals(err, test.expectedError) {
				t.Errorf("Expected error: %v, got: %v", test.expectedError, err)
			}
		})
	}
}

func Test_getSpannerColumnType(t *testing.T) {
	type args struct {
		c string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "convert text to string",
			args: args{c: "text"},
			want: "string",
		},
		{
			name: "convert blob to bytes",
			args: args{c: "blob"},
			want: "bytes",
		},
		{
			name: "convert timestamp to timestamp",
			args: args{c: "timestamp"},
			want: "timestamp",
		},
		{
			name: "convert int to int64",
			args: args{c: "int"},
			want: "int64",
		},
		{
			name: "convert bigint to int64",
			args: args{c: "bigint"},
			want: "int64",
		},
		{
			name: "convert boolean to bool",
			args: args{c: "boolean"},
			want: "bool",
		},
		{
			name: "convert uuid to string",
			args: args{c: "uuid"},
			want: "string",
		},
		{
			name: "convert float to float64",
			args: args{c: "float"},
			want: "float64",
		},
		{
			name: "convert double to float64",
			args: args{c: "double"},
			want: "float64",
		},
		{
			name: "convert map<text, boolean> to array[string, bool]",
			args: args{c: "map<text, boolean>"},
			want: "array[string, bool]",
		},
		{
			name: "convert map<text, text> to array[string, string]",
			args: args{c: "map<text, text>"},
			want: "array[string, string]",
		},
		{
			name: "convert map<text, timestamp> to array[string, timestamp]",
			args: args{c: "map<text, timestamp>"},
			want: "array[string, timestamp]",
		},
		{
			name: "convert set<text> to array[string]",
			args: args{c: "set<text>"},
			want: "array[string]",
		},
		{
			name: "convert list<text> to array[string]",
			args: args{c: "list<text>"},
			want: "array[string]",
		},
		{
			name: "convert frozen<list<text>> to array[string]",
			args: args{c: "frozen<list<text>>"},
			want: "array[string]",
		},
		{
			name: "convert frozen<set<text>> to array[string]",
			args: args{c: "frozen<set<text>>"},
			want: "array[string]",
		},
		{
			name: "unknown type",
			args: args{c: "unknown"},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetSpannerColumnType(tt.args.c); got != tt.want {
				t.Errorf("getSpannerColumnType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetCassandraColumnType(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		wantType datatype.DataType
		wantErr  bool
	}{
		{"Text Type", "text", datatype.Varchar, false},
		{"Bolob Type", "blob", datatype.Blob, false},
		{"Timestamp Type", "timestamp", datatype.Timestamp, false},
		{"Int Type", "int", datatype.Int, false},
		{"Float Type", "float", datatype.Float, false},
		{"Bigint Type", "bigint", datatype.Bigint, false},
		{"Boolean Type", "boolean", datatype.Boolean, false},
		{"Uuid Type", "uuid", datatype.Uuid, false},
		{"map<text, boolean> Type", "map<text, boolean>", datatype.NewMapType(datatype.Varchar, datatype.Boolean), false},
		{"map<text, text> Type", "map<text, text>", datatype.NewMapType(datatype.Varchar, datatype.Varchar), false},
		{"map<text, timestamp> Type", "map<text, timestamp>", datatype.NewMapType(datatype.Varchar, datatype.Timestamp), false},
		{"list<text> Type", "list<text>", datatype.NewListType(datatype.Varchar), false},
		{"frozen<list<text>> Type", "frozen<list<text>>", datatype.NewListType(datatype.Varchar), false},
		{"set<text> Type", "set<text>", datatype.NewSetType(datatype.Varchar), false},
		{"frozen<set<text>> Type", "frozen<set<text>>", datatype.NewSetType(datatype.Varchar), false},
		{"Invalid Type", "unknown", nil, true},
		// Future scope items below:
		// {"map<text, int> Type", "map<text, int>", datatype.NewMapType(datatype.Varchar, datatype.Int), false},
		// {"map<text, int64> Type", "map<text, int64>", datatype.NewMapType(datatype.Varchar, datatype.Bigint), false},
		// {"map<text, float64> Type", "map<text, float64>", datatype.NewMapType(datatype.Varchar, datatype.Double), false},
		// {"map<text, date> Type", "map<text, date>", datatype.NewMapType(datatype.Varchar, datatype.Date), false},
		// {"map<text, uuid> Type", "map<text, uuid>", datatype.NewMapType(datatype.Varchar, datatype.Uuid), false},
		// {"list<boolean> Type", "list<boolean>", datatype.NewListType(datatype.Boolean), false},
		// {"list<int> Type", "list<int>", datatype.NewListType(datatype.Int), false},
		// {"list<int64> Type", "list<int64>", datatype.NewListType(datatype.Bigint), false},
		// {"list<float64> Type", "list<float64>", datatype.NewListType(datatype.Double), false},
		// {"list<date> Type", "list<date>", datatype.NewListType(datatype.Date), false},
		// {"list<timestamp> Type", "list<timestamp>", datatype.NewListType(datatype.Timestamp), false},
		// {"set<boolean> Type", "set<boolean>", datatype.NewSetType(datatype.Boolean), false},
		// {"set<int> Type", "set<int>", datatype.NewSetType(datatype.Int), false},
		// {"set<int64> Type", "set<int64>", datatype.NewSetType(datatype.Bigint), false},
		// {"set<float64> Type", "set<float64>", datatype.NewSetType(datatype.Double), false},
		// {"set<date> Type", "set<date>", datatype.NewSetType(datatype.Date), false},
		// {"set<timestamp> Type", "set<timestamp>", datatype.NewSetType(datatype.Timestamp), false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotType, err := GetCassandraColumnType(tc.input)
			if (err != nil) != tc.wantErr {
				t.Errorf("getCassandraColumnType(%s) error = %v, wantErr %v", tc.input, err, tc.wantErr)
				return
			}

			if err == nil && !reflect.DeepEqual(gotType, tc.wantType) {
				t.Errorf("getCassandraColumnType(%s) = %v, want %v", tc.input, gotType, tc.wantType)
			}
		})
	}
}

func errorEquals(err1, err2 error) bool {
	if (err1 == nil && err2 != nil) || (err1 != nil && err2 == nil) {
		return false
	}
	if err1 != nil && err2 != nil && err1.Error() != err2.Error() {
		return false
	}
	return true
}

func TestTypeConversion(t *testing.T) {
	protocalV := primitive.ProtocolVersion4
	tests := []struct {
		name            string
		input           interface{}
		expected        []byte
		wantErr         bool
		protocalVersion primitive.ProtocolVersion
	}{
		{
			name:            "String",
			input:           "example string",
			expected:        []byte{'e', 'x', 'a', 'm', 'p', 'l', 'e', ' ', 's', 't', 'r', 'i', 'n', 'g'},
			protocalVersion: protocalV,
		},
		{
			name:            "Int64",
			input:           int64(12345),
			expected:        []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x30, 0x39},
			protocalVersion: protocalV,
		},
		{
			name:            "Boolean",
			input:           true,
			expected:        []byte{0x01},
			protocalVersion: protocalV,
		},
		{
			name:            "Float64",
			input:           123.45,
			expected:        []byte{0x40, 0x5E, 0xDC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCD},
			protocalVersion: protocalV,
		},
		{
			name:            "NullDate",
			input:           spanner.NullDate{Valid: true, Date: civil.Date{Year: 2021, Month: time.January, Day: 1}},
			expected:        []byte{0x80, 0x00, 0x48, 0xC4},
			protocalVersion: protocalV,
		},
		{
			name:            "Timestamp",
			input:           time.Date(2021, time.April, 10, 12, 0, 0, 0, time.UTC),
			expected:        []byte{0x00, 0x00, 0x01, 0x78, 0xBB, 0xA7, 0x32, 0x00},
			protocalVersion: protocalV,
		},
		{
			name:            "Byte",
			input:           []byte{0x01, 0x02, 0x03, 0x04},
			expected:        []byte{0x01, 0x02, 0x03, 0x04},
			protocalVersion: protocalV,
		},
		{
			name:            "String Error Case",
			input:           struct{}{},
			wantErr:         true,
			protocalVersion: protocalV,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TypeConversion(tt.input, tt.protocalVersion)
			if (err != nil) != tt.wantErr {
				t.Errorf("TypeConversion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("TypeConversion(%v) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// TestParseDurationToSeconds tests the parseDurationToSeconds function for various cases.
func TestParseDurationToSeconds(t *testing.T) {
	// Define test cases
	tests := []struct {
		input          string
		expectedOutput int64
		expectError    bool
	}{
		{"1s", 1, false},     // Valid seconds
		{"60s", 60, false},   // Valid seconds
		{"1m", 60, false},    // Valid minutes
		{"2m", 120, false},   // Valid minutes
		{"1h", 3600, false},  // Valid hours
		{"3h", 10800, false}, // Valid hours
		{"", 0, true},        // Empty string
		{"1", 0, true},       // Missing unit
		{"1x", 0, true},      // Invalid unit
		{"invalid", 0, true}, // Invalid format
		{"1.5h", 0, true},    // Non-integer value
		{"-1m", 0, true},     // Negative value
		{"5d", 0, true},      // Unsupported unit
	}

	// Run test cases
	for _, test := range tests {
		output, err := ParseDurationToSeconds(test.input)
		if test.expectError && err == nil {
			t.Errorf("Expected error for input %s, but got none", test.input)
		} else if !test.expectError && err != nil {
			t.Errorf("Did not expect error for input %s, but got %v", test.input, err)
		} else if output != test.expectedOutput {
			t.Errorf("Expected output %d for input %s, but got %d", test.expectedOutput, test.input, output)
		}
	}
}
