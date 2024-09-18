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
package responsehandler_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/responsehandler"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/proxycore"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/utilities"
	"go.uber.org/zap"
)

func TestGetColumnMeta(t *testing.T) {
	mockTableConfig := &tableConfig.TableConfig{
		TablesMetaData: map[string]map[string]*tableConfig.Column{
			"test_table": {
				"test_event": {
					Name:    "test_event",
					CQLType: "blob",
				},
			},
		},
		Logger: zap.NewNop(),
	}

	tests := []struct {
		name        string
		columnName  string
		query       responsehandler.QueryMetadata
		wantMetaStr string
		wantErr     bool
	}{
		{
			name:        "Valid Query and Column",
			columnName:  "test_event",
			query:       responsehandler.QueryMetadata{Query: "SELECT * FROM key_space.test_table", TableName: "test_table", KeyspaceName: "key_space"},
			wantMetaStr: "blob",
			wantErr:     false,
		},
		{
			name:        "Non-Existent Column",
			columnName:  "non_existent_column",
			query:       responsehandler.QueryMetadata{Query: "SELECT * FROM key_space.test_table", TableName: "test_table", KeyspaceName: "key_space"},
			wantMetaStr: "",
			wantErr:     true,
		},
		{
			name:        "Non-Existent Table",
			columnName:  "test_event",
			query:       responsehandler.QueryMetadata{Query: "SELECT * FROM key_space.non_existent_table", TableName: "non_existent_table"},
			wantMetaStr: "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			th := &responsehandler.TypeHandler{TableConfig: mockTableConfig}

			gotMetaStr, err := th.GetColumnMeta(tt.columnName, tt.query.TableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetColumnMeta() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotMetaStr != tt.wantMetaStr {
				t.Errorf("GetColumnMeta() = %v, want %v", gotMetaStr, tt.wantMetaStr)
			}
		})
	}
}

func TestTypeHandler_TypeConversionForWriteTime(t *testing.T) {
	// Mock logger implementation
	th := &responsehandler.TypeHandler{Logger: zap.NewNop(), ProtocalV: primitive.ProtocolVersion4}

	tests := []struct {
		name          string
		timeValue     time.Time
		expectedBytes []byte
		expectedErr   error
	}{
		{
			name:          "Valid time.Time value",
			timeValue:     time.Date(2024, time.May, 14, 12, 30, 0, 0, time.UTC),
			expectedBytes: []byte{0x0, 0x6, 0x18, 0x69, 0x29, 0x12, 0xe2, 0x0}, // Expected bytes
			expectedErr:   nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resultBytes, resultErr := th.TypeConversionForWriteTime(test.timeValue)

			if resultErr != nil && test.expectedErr == nil {
				t.Errorf("Expected no error, but got error: %v", resultErr)
			} else if resultErr == nil && test.expectedErr != nil {
				t.Errorf("Expected error %v, but got no error", test.expectedErr)
			} else if resultErr != nil && test.expectedErr != nil && resultErr.Error() != test.expectedErr.Error() {
				t.Errorf("Expected error %v, but got error: %v", test.expectedErr, resultErr)
			}

			if len(resultBytes) != len(test.expectedBytes) {
				t.Errorf("Expected byte slice length %d, but got %d", len(test.expectedBytes), len(resultBytes))
			}

			for i := range resultBytes {
				if resultBytes[i] != test.expectedBytes[i] {
					t.Errorf("Byte at index %d: Expected %x, but got %x", i, test.expectedBytes[i], resultBytes[i])
				}
			}
		})
	}
}

func TestBuildResponseForSystemQueries(t *testing.T) {
	th := &responsehandler.TypeHandler{Logger: zap.NewNop()}
	protocolV := primitive.ProtocolVersion4

	tests := []struct {
		name     string
		rows     [][]interface{}
		expected []message.Row
		wantErr  bool
	}{
		{
			name: "Single Row with Mixed Types",
			rows: [][]interface{}{
				{"example string", int64(12345), true, 123.45, time.Date(2021, time.April, 10, 12, 0, 0, 0, time.UTC)},
			},
			expected: []message.Row{
				{
					[]byte{'e', 'x', 'a', 'm', 'p', 'l', 'e', ' ', 's', 't', 'r', 'i', 'n', 'g'},
					[]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x30, 0x39},
					[]byte{0x01},
					[]byte{0x40, 0x5E, 0xDC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCD},
					[]byte{0x00, 0x00, 0x01, 0x78, 0xBB, 0xA7, 0x32, 0x00},
				},
			},
			wantErr: false,
		},
		{
			name: "Error in Row Conversion",
			rows: [][]interface{}{
				{"example string", struct{}{}, true},
			},
			expected: nil,
			wantErr:  true,
		},
		{
			name: "Multiple Rows",
			rows: [][]interface{}{
				{"example string", int64(12345)},
				{true, 123.45},
			},
			expected: []message.Row{
				{
					[]byte{'e', 'x', 'a', 'm', 'p', 'l', 'e', ' ', 's', 't', 'r', 'i', 'n', 'g'},
					[]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x30, 0x39},
				},
				{
					[]byte{0x01},
					[]byte{0x40, 0x5E, 0xDC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCD},
				},
			},
			wantErr: false,
		},
		{
			name: "map type",
			rows: [][]interface{}{
				{"example string", true, map[string]string{
					"class":              "org.apache.cassandra.locator.SimpleStrategy",
					"replication_factor": "1",
				}},
			},
			expected: []message.Row{
				{
					[]byte{'e', 'x', 'a', 'm', 'p', 'l', 'e', ' ', 's', 't', 'r', 'i', 'n', 'g'},
					[]byte{0x01},
					mustEncodeType(map[string]string{
						"class":              "org.apache.cassandra.locator.SimpleStrategy",
						"replication_factor": "1",
					}, protocolV),
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := th.BuildResponseForSystemQueries(tt.rows, protocolV)
			if tt.name == "map type" {
				if len(got) != len(tt.expected) {
					t.Errorf("BuildResponseForSystemQueries() = %v rows, want %v rows", len(got), len(tt.expected))
				} else if getSizeOf3DByteSlice(got) != getSizeOf3DByteSlice(tt.expected) {
					t.Errorf("BuildResponseForSystemQueries() = %v rows, want %v rows", len(got), len(tt.expected))
				}
			} else {
				if (err != nil) != tt.wantErr {
					t.Errorf("BuildResponseForSystemQueries() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				if !reflect.DeepEqual(got, tt.expected) {
					t.Errorf("BuildResponseForSystemQueries() = %v, want %v", got, tt.expected)
				}

				// Additional debug prints for failed test cases
				if (err != nil) && tt.wantErr {
					fmt.Printf("Expected error occurred: %v\n", err)
				}
			}
		})
	}
}

func mustEncodeType(val interface{}, protocolV primitive.ProtocolVersion) []byte {
	bytes, err := proxycore.EncodeType(datatype.NewMapType(datatype.Varchar, datatype.Varchar), protocolV, val)
	if err != nil {
		panic(err)
	}
	return bytes
}

func getSizeOf3DByteSlice(data [][][]byte) int {
	totalSize := 0
	for _, twoDSlice := range data {
		for _, oneDSlice := range twoDSlice {
			totalSize += len(oneDSlice)
		}
	}
	return totalSize
}

func TestBuildColumnMetadata2(t *testing.T) {
	logger, _ := zap.NewProduction()
	th := &responsehandler.TypeHandler{
		Logger: logger,
		TableConfig: &tableConfig.TableConfig{
			TablesMetaData: map[string]map[string]*tableConfig.Column{
				"table1": {
					"column1": {
						Name:    "column1",
						CQLType: "bigint",
					},
					"column2": {
						Name:    "column2",
						CQLType: "text",
					},
					"column3": {
						Name:    "column3",
						CQLType: "boolean",
					},
				},
			},
			Logger: zap.NewNop(),
		},
		ProtocalV: primitive.ProtocolVersion4,
	}

	rowType := &spannerpb.StructType{
		Fields: []*spannerpb.StructType_Field{
			{Name: "column1", Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64}},
			{Name: "column2", Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING}},
			{Name: "spanner_ttl_ts", Type: &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP}},
			{Name: "column3", Type: &spannerpb.Type{Code: spannerpb.TypeCode_BOOL}},
		},
	}

	aliasMap := map[string]tableConfig.AsKeywordMeta{
		"column1": {CQLType: "bigint", DataType: datatype.Bigint},
		"column2": {CQLType: "text", DataType: datatype.Varchar},
	}

	rowFuncs, cmd, err := th.BuildColumnMetadata(rowType, primitive.ProtocolVersion4, aliasMap, "table1", "keyspace1")

	if err != nil {
		t.Fatalf("Expected no error, but got: %v", err)
	}

	if len(rowFuncs) != 3 {
		t.Fatalf("Expected 2 rowFuncs, but got: %d", len(rowFuncs))
	}

	if len(cmd) != 3 {
		t.Fatalf("Expected 2 column metadata entries, but got: %d", len(cmd))
	}

	expectedCmd := []*message.ColumnMetadata{
		{Keyspace: "keyspace1", Table: "table1", Name: "column1", Index: 0, Type: datatype.Bigint},
		{Keyspace: "keyspace1", Table: "table1", Name: "column2", Index: 1, Type: datatype.Varchar},
		{Keyspace: "keyspace1", Table: "table1", Name: "column3", Index: 2, Type: datatype.Boolean},
	}

	for i, col := range cmd {
		if col.Keyspace != expectedCmd[i].Keyspace || col.Table != expectedCmd[i].Table || col.Name != expectedCmd[i].Name || col.Type.String() != expectedCmd[i].Type.String() {
			t.Errorf("Unexpected column metadata at index %d: %+v", i, col)
		}
	}
}

func TestBuildColumnMetadata(t *testing.T) {
	logger, _ := zap.NewProduction()
	th := &responsehandler.TypeHandler{
		Logger: logger,
		TableConfig: &tableConfig.TableConfig{
			TablesMetaData: map[string]map[string]*tableConfig.Column{
				"table1": {
					"column1": {
						Name:    "column1",
						CQLType: "bigint",
					},
					"column2": {
						Name:    "column2",
						CQLType: "text",
					},
					"column3": {
						Name:    "column3",
						CQLType: "boolean",
					},
					"column4": {
						Name:    "column4",
						CQLType: "float",
					},
					"column5": {
						Name:    "column5",
						CQLType: "double",
					},
					"column6": {
						Name:    "column6",
						CQLType: "blob",
					},
					"column7": {
						Name:    "column7",
						CQLType: "date",
					},
					"column8": {
						Name:    "column8",
						CQLType: "timestamp",
					},
					"column9": {
						Name:    "column9",
						CQLType: "bigint",
					},
					"column10": {
						Name:    "column10",
						CQLType: "text",
					},
					"column11": {
						Name:    "column11",
						CQLType: "uuid",
					},
				},
			},
			Logger: zap.NewNop(),
		},
		ProtocalV: primitive.ProtocolVersion4,
	}

	rowType := &spannerpb.StructType{
		Fields: []*spannerpb.StructType_Field{
			{Name: "column1", Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64}},
			{Name: "column2", Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING}},
			{Name: "spanner_ttl_ts", Type: &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP}},
			{Name: "column3", Type: &spannerpb.Type{Code: spannerpb.TypeCode_BOOL}},
			{Name: "column4", Type: &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT64}},
			{Name: "column5", Type: &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT64}},
			{Name: "column6", Type: &spannerpb.Type{Code: spannerpb.TypeCode_BYTES}},
			{Name: "column7", Type: &spannerpb.Type{Code: spannerpb.TypeCode_DATE}},
			{Name: "column8", Type: &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP}},
			{Name: "column9", Type: &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP}},
			{Name: "column10", Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING}},
			{Name: "column11", Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING}},
		},
	}

	aliasMap := map[string]tableConfig.AsKeywordMeta{
		"column1":  {CQLType: "bigint", DataType: datatype.Bigint},
		"column2":  {CQLType: "text", DataType: datatype.Varchar},
		"column3":  {CQLType: "boolean", DataType: datatype.Boolean},
		"column4":  {CQLType: "float", DataType: datatype.Float},
		"column5":  {CQLType: "double", DataType: datatype.Double},
		"column6":  {CQLType: "blob", DataType: datatype.Blob},
		"column7":  {CQLType: "date", DataType: datatype.Date},
		"column8":  {CQLType: "timestamp", DataType: datatype.Timestamp},
		"column9":  {CQLType: "bigint", DataType: datatype.Bigint},
		"column10": {CQLType: "text", DataType: datatype.Varchar},
		"column11": {CQLType: "uuid", DataType: datatype.Uuid},
	}

	rowFuncs, cmd, err := th.BuildColumnMetadata(rowType, primitive.ProtocolVersion4, aliasMap, "table1", "keyspace1")

	if err != nil {
		t.Fatalf("Expected no error, but got: %v", err)
	}

	if len(rowFuncs) != 11 { // Excluding spannerTTLColumn and spannerTSColumn
		t.Fatalf("Expected 10 rowFuncs, but got: %d", len(rowFuncs))
	}

	if len(cmd) != 11 { // Excluding spannerTTLColumn and spannerTSColumn
		t.Fatalf("Expected 10 column metadata entries, but got: %d", len(cmd))
	}

	expectedCmd := []*message.ColumnMetadata{
		{Keyspace: "keyspace1", Table: "table1", Name: "column1", Index: 0, Type: datatype.Bigint},
		{Keyspace: "keyspace1", Table: "table1", Name: "column2", Index: 1, Type: datatype.Varchar},
		{Keyspace: "keyspace1", Table: "table1", Name: "column3", Index: 3, Type: datatype.Boolean},
		{Keyspace: "keyspace1", Table: "table1", Name: "column4", Index: 4, Type: datatype.Float},
		{Keyspace: "keyspace1", Table: "table1", Name: "column5", Index: 5, Type: datatype.Double},
		{Keyspace: "keyspace1", Table: "table1", Name: "column6", Index: 6, Type: datatype.Blob},
		{Keyspace: "keyspace1", Table: "table1", Name: "column7", Index: 7, Type: datatype.Date},
		{Keyspace: "keyspace1", Table: "table1", Name: "column8", Index: 8, Type: datatype.Timestamp},
		{Keyspace: "keyspace1", Table: "table1", Name: "column9", Index: 9, Type: datatype.Bigint},
		{Keyspace: "keyspace1", Table: "table1", Name: "column10", Index: 10, Type: datatype.Varchar},
		{Keyspace: "keyspace1", Table: "table1", Name: "column11", Index: 11, Type: datatype.Uuid},
	}

	for i, col := range cmd {
		if col.Keyspace != expectedCmd[i].Keyspace || col.Table != expectedCmd[i].Table || col.Name != expectedCmd[i].Name || col.Type.String() != expectedCmd[i].Type.String() {
			t.Errorf("Unexpected column metadata at index %d: %+v", i, col)
		}
	}
}

func TestBuildColumnMetadataArrayAndSetTypes(t *testing.T) {
	logger, _ := zap.NewProduction()
	th := &responsehandler.TypeHandler{
		Logger: logger,
		TableConfig: &tableConfig.TableConfig{
			TablesMetaData: map[string]map[string]*tableConfig.Column{
				"table2": {
					"column1": {Name: "column1", CQLType: "list<bigint>"},
					"column2": {Name: "column2", CQLType: "set<text>"},
				},
			},
			Logger: zap.NewNop(),
		},
		ProtocalV: primitive.ProtocolVersion4,
	}

	rowType := &spannerpb.StructType{
		Fields: []*spannerpb.StructType_Field{
			{Name: "column1", Type: &spannerpb.Type{Code: spannerpb.TypeCode_ARRAY, ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_ARRAY}}},
			{Name: "column2", Type: &spannerpb.Type{Code: spannerpb.TypeCode_ARRAY, ArrayElementType: &spannerpb.Type{Code: spannerpb.TypeCode_ARRAY}}},
		},
	}

	aliasMap := map[string]tableConfig.AsKeywordMeta{
		"column1": {CQLType: "list<bigint>", DataType: utilities.ListBigintCassandraType},
		"column2": {CQLType: "set<text>", DataType: datatype.Varchar},
	}

	rowFuncs, cmd, err := th.BuildColumnMetadata(rowType, primitive.ProtocolVersion4, aliasMap, "table2", "keyspace2")

	if err != nil {
		t.Fatalf("Expected no error, but got: %v", err)
	}

	if len(rowFuncs) != 2 {
		t.Fatalf("Expected 2 rowFuncs, but got: %d", len(rowFuncs))
	}

	if len(cmd) != 2 {
		t.Fatalf("Expected 2 column metadata entries, but got: %d", len(cmd))
	}

	expectedCmd := []*message.ColumnMetadata{
		{Keyspace: "keyspace2", Table: "table2", Name: "column1", Index: 0, Type: datatype.NewListType(datatype.Bigint)},
		{Keyspace: "keyspace2", Table: "table2", Name: "column2", Index: 1, Type: datatype.NewSetType(datatype.Varchar)},
	}

	for i, col := range cmd {
		if col.Keyspace != expectedCmd[i].Keyspace || col.Table != expectedCmd[i].Table || col.Name != expectedCmd[i].Name || col.Type.String() != expectedCmd[i].Type.String() {
			t.Errorf("Unexpected column metadata at index %d: %+v", i, col)
		}
	}
}

func TestBuildColumnMetadataMapTypes(t *testing.T) {
	logger, _ := zap.NewProduction()
	th := &responsehandler.TypeHandler{
		Logger: logger,
		TableConfig: &tableConfig.TableConfig{
			TablesMetaData: map[string]map[string]*tableConfig.Column{
				"table3": {
					"column1": {Name: "column1", CQLType: "map<text, bigint>"},
					"column2": {Name: "column2", CQLType: "map<text, boolean>"},
				},
			},
			Logger: zap.NewNop(),
		},
		ProtocalV: primitive.ProtocolVersion4,
	}

	rowType := &spannerpb.StructType{
		Fields: []*spannerpb.StructType_Field{
			{Name: "column1", Type: &spannerpb.Type{Code: spannerpb.TypeCode_JSON}},
			{Name: "AliasX", Type: &spannerpb.Type{Code: spannerpb.TypeCode_JSON}},
		},
	}

	aliasMap := map[string]tableConfig.AsKeywordMeta{
		"column1": {CQLType: "map<text, bigint>", DataType: utilities.MapBigintCassandraType},
		"AliasX":  {CQLType: "map<text, boolean>", DataType: utilities.ListBooleanCassandraType},
	}

	rowFuncs, cmd, err := th.BuildColumnMetadata(rowType, primitive.ProtocolVersion4, aliasMap, "table3", "keyspace3")

	if err != nil {
		t.Fatalf("Expected no error, but got: %v", err)
	}

	if len(rowFuncs) != 2 {
		t.Fatalf("Expected 2 rowFuncs, but got: %d", len(rowFuncs))
	}

	if len(cmd) != 2 {
		t.Fatalf("Expected 2 column metadata entries, but got: %d", len(cmd))
	}

	expectedCmd := []*message.ColumnMetadata{
		{Keyspace: "keyspace3", Table: "table3", Name: "column1", Index: 0, Type: datatype.NewMapType(datatype.Varchar, datatype.Bigint)},
		{Keyspace: "keyspace3", Table: "table3", Name: "AliasX", Index: 1, Type: datatype.NewMapType(datatype.Varchar, datatype.Boolean)},
	}

	for i, col := range cmd {
		if col.Keyspace != expectedCmd[i].Keyspace || col.Table != expectedCmd[i].Table || col.Name != expectedCmd[i].Name || col.Type.String() != expectedCmd[i].Type.String() {
			t.Errorf("Unexpected column metadata at index %d: %+v", i, col)
		}
	}
	// scenario with Empty alias map
	aliasMap = map[string]tableConfig.AsKeywordMeta{}

	_, _, err = th.BuildColumnMetadata(rowType, primitive.ProtocolVersion4, aliasMap, "table3", "keyspace3")

	if err == nil {
		t.Fatalf("Expected error, but got: None")
	}
}
