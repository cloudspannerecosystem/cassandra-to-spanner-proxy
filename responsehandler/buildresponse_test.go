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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/responsehandler"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/proxycore"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/utilities"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTypeHandler_BuildResponseRow(t *testing.T) {
	mockTableConfig := &tableConfig.TableConfig{
		TablesMetaData: map[string]map[string]*tableConfig.Column{
			"test_table": {
				"test_hash": &tableConfig.Column{
					Name:    "test_hash",
					CQLType: "text",
				},
				"test_guid": &tableConfig.Column{
					Name:    "test_guid",
					CQLType: "uuid",
				},
				"test_attributes": &tableConfig.Column{
					Name:    "test_attributes",
					CQLType: "frozen<list<text>>",
				},
				"endpoints": &tableConfig.Column{
					Name:    "endpoints",
					CQLType: "frozen<set<text>>",
				},
				"test_ids": &tableConfig.Column{
					Name:    "test_ids",
					CQLType: "map<text, boolean>",
				},
				"example_int_column": &tableConfig.Column{
					Name:    "example_int_column",
					CQLType: "bigint",
				},
				"example_float_column": &tableConfig.Column{
					Name:    "example_float_column",
					CQLType: "double",
				},
				"example_bool_column": &tableConfig.Column{
					Name:    "example_bool_column",
					CQLType: "boolean",
				},
				"example_bytes_column": &tableConfig.Column{
					Name:    "example_bytes_column",
					CQLType: "blob",
				},
				"example_date_column": &tableConfig.Column{
					Name:    "example_date_column",
					CQLType: "date",
				},
				"example_timestamp_column": &tableConfig.Column{
					Name:    "example_timestamp_column",
					CQLType: "timestamp",
				},
				"example_int32_column": &tableConfig.Column{
					Name:    "example_int32_column",
					CQLType: "int",
				},
				"test_bigint_array": &tableConfig.Column{
					Name:    "test_bigint_array",
					CQLType: "list<bigint>",
				},
				"test_set_bigint": &tableConfig.Column{
					Name:    "test_bigint_array",
					CQLType: "set<bigint>",
				},
			},
		},
		Logger: zap.NewNop(),
	}
	th := &responsehandler.TypeHandler{TableConfig: mockTableConfig, Logger: zap.NewNop()}

	tests := []struct {
		name                     string
		row                      *spanner.Row
		rowType                  *spannerpb.StructType
		query                    responsehandler.QueryMetadata
		isColumnMetadataCaptured bool
		expectedRow              message.Row
		expectedColumnMetadata   []*message.ColumnMetadata
		wantErr                  bool
	}{
		{
			name:                     "Successful INT64 Conversion",
			row:                      mockSpannerRowWithInt64(),
			rowType:                  mockSpannerStructTypeWithInt64(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              expectedMessageRowForInt64(),
			expectedColumnMetadata:   expectedColumnMetadataForInt64(),
			wantErr:                  false,
		},
		{
			name:                     "Error Retrieving Data",
			row:                      mockSpannerRowWithError(),
			rowType:                  mockSpannerStructTypeWithInt64(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			wantErr:                  true,
		},
		{
			name:                     "Successful FLOAT64 Conversion",
			row:                      mockSpannerRowWithFloat64(),
			rowType:                  mockSpannerStructTypeWithFloat64(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              expectedMessageRowForFloat64(),
			expectedColumnMetadata:   expectedColumnMetadataForFloat64(),
			wantErr:                  false,
		},
		{
			name:                     "Error Retrieving Float64 Data",
			row:                      mockSpannerRowWithFloat64TypeError(),
			rowType:                  mockSpannerStructTypeWithFloat64(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			wantErr:                  true,
		},
		{
			name:                     "Successful BOOL Conversion",
			row:                      mockSpannerRowWithBool(),
			rowType:                  mockSpannerStructTypeWithBool(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              expectedMessageRowForBool(),
			expectedColumnMetadata:   expectedColumnMetadataForBool(),
			wantErr:                  false,
		},
		{
			name:                     "Error Retrieving Bool Data",
			row:                      mockSpannerRowWithBoolTypeError(),
			rowType:                  mockSpannerStructTypeWithBool(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			wantErr:                  true,
		},
		{
			name:                     "Successful BYTES Conversion",
			row:                      mockSpannerRowWithBytes(),
			rowType:                  mockSpannerStructTypeWithBytes(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              expectedMessageRowForBytes(),
			expectedColumnMetadata:   expectedColumnMetadataForBytes(),
			wantErr:                  false,
		},
		{
			name:                     "Error Retrieving Bytes Data",
			row:                      mockSpannerRowWithBytesTypeError(),
			rowType:                  mockSpannerStructTypeWithBytes(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			wantErr:                  true,
		},
		{
			name:                     "Successful DATE Conversion",
			row:                      mockSpannerRowWithDate(),
			rowType:                  mockSpannerStructTypeWithDate(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              expectedMessageRowForDate(),
			expectedColumnMetadata:   expectedColumnMetadataForDate(),
			wantErr:                  false,
		},
		{
			name:                     "Error Retrieving Date Data",
			row:                      mockSpannerRowWithDateTypeError(),
			rowType:                  mockSpannerStructTypeWithDate(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			wantErr:                  true,
		},
		{
			name:                     "Successful TIMESTAMP Conversion",
			row:                      mockSpannerRowWithTimestamp(),
			rowType:                  mockSpannerStructTypeWithTimestamp(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              expectedMessageRowForTimestamp(),
			expectedColumnMetadata:   expectedColumnMetadataForTimestamp(),
			wantErr:                  false,
		},
		{
			name:                     "Error Retrieving Timestamp Data",
			row:                      mockSpannerRowWithTimestampTypeError(),
			rowType:                  mockSpannerStructTypeWithTimestamp(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			wantErr:                  true,
		},
		{
			name:                     "Successful String Conversion",
			row:                      mockSpannerRowWithString(),
			rowType:                  mockSpannerStructTypeWithString(),
			query:                    mockStringQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              expectedMessageRowForString(),
			expectedColumnMetadata:   expectedColumnMetadataForString(),
			wantErr:                  false,
		},
		{
			name:                     "Successful UUID String Conversion",
			row:                      mockSpannerRowWithUUIDString(),
			rowType:                  mockSpannerStructTypeWithUUIDString(),
			query:                    mockStringQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              expectedMessageRowForUUID(),
			expectedColumnMetadata:   expectedColumnMetadataForUUID(),
			wantErr:                  false,
		},
		{
			name:                     "Successful Array (Int64) List Conversion",
			row:                      mockSpannerRowWithIntArray(),
			rowType:                  mockSpannerStructTypeWithIntArray(),
			query:                    mockStringQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              expectedMessageRowForIntArray(),
			expectedColumnMetadata:   expectedColumnMetadataForIntArray(),
			wantErr:                  false,
		},
		{
			name:                     "Successful Array (Int64) Set Conversion",
			row:                      mockSpannerRowWithIntArrayForSet(),
			rowType:                  mockSpannerStructTypeWithIntArrayForSet(),
			query:                    mockStringQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              expectedMessageRowForIntArray(),
			expectedColumnMetadata:   expectedColumnMetadataForIntArrayForSet(),
			wantErr:                  false,
		},
		{
			name:                     "String Conversion Error",
			row:                      mockSpannerRowWithStringError(),
			rowType:                  mockSpannerStructTypeWithString(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              nil,
			expectedColumnMetadata:   nil,
			wantErr:                  true,
		},
		{
			name:                     "Successful Json->Map(bytes) Conversion",
			row:                      mockSpannerRowWithJson(),
			rowType:                  mockSpannerStructTypeWithJSON(),
			query:                    mockStringQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              expectedMessageRowForJson(),
			expectedColumnMetadata:   expectedColumnMetadataForJson(),
			wantErr:                  false,
		},
		{
			name:                     "Successful int32 Conversion",
			row:                      mockSpannerRowWithInt(),
			rowType:                  mockSpannerStructTypeWithInt(),
			query:                    mockQueryMetadata(),
			isColumnMetadataCaptured: false,
			expectedRow:              expectedMessageRowForInt(),
			expectedColumnMetadata:   expectedColumnMetadataForInt32(),
			wantErr:                  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowFuncs, gotColumnMetadata, err := th.BuildColumnMetadata(tt.rowType, tt.query.ProtocalV, tt.query.AliasMap, tt.query.TableName, tt.query.KeyspaceName)
			if !tt.wantErr && err != nil {
				t.Errorf("Error while fetching metadata -case %s -  %v", tt.name, err)
			}
			gotRow, err := th.BuildResponseRow(tt.row, rowFuncs)
			if (err != nil) != tt.wantErr {
				t.Errorf("TypeHandler.BuildResponseRow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if tt.name == "Successful Json->Map(bytes) Conversion" {
					jsonStr := "{\"math\": true, \"history\": false, \"english\": true}"
					decodedInterface, err := proxycore.DecodeType(datatype.NewMapType(datatype.Varchar, datatype.Boolean), 4, gotRow[0])
					if err != nil {
						t.Fatalf("Error decoding gotRow: %v", err)
					}
					if gotMap, ok := decodedInterface.(map[*string]*bool); !ok {
						t.Fatalf("Failed to assert decodedInterface as map[*string]*bool")
					} else {
						convertedGotMap := make(map[string]bool)
						for key, val := range gotMap {
							if key != nil && val != nil {
								convertedGotMap[*key] = *val
							}
						}
						var wantMap map[string]bool
						err := json.Unmarshal([]byte(jsonStr), &wantMap)
						if err != nil {
							t.Error(err.Error())
						}
						if !reflect.DeepEqual(convertedGotMap, wantMap) {
							t.Errorf("Maps do not match. got: %v, want: %v", convertedGotMap, wantMap)
						}
					}
				} else {
					if !reflect.DeepEqual(gotRow, tt.expectedRow) {
						t.Errorf("TypeHandler.BuildResponseRow() gotRow = %v, want %v", gotRow, tt.expectedRow)
					}

					if !reflect.DeepEqual(gotColumnMetadata, tt.expectedColumnMetadata) {
						t.Errorf("TypeHandler.BuildResponseRow() gotColumnMetadata = %v, want %v", gotColumnMetadata[0], tt.expectedColumnMetadata[0])
					}
				}

			}
		})
	}
}

type ColumnMetadata struct {
	Keyspace string
	Table    string
	Name     string
	Index    int32
	Type     datatype.DataType
}

// Mock Row:
func mockSpannerRowWithInt64() *spanner.Row {
	columnNames := []string{"example_int_column"}
	columnValues := []interface{}{int64(12345)}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic(status.Errorf(codes.Internal, "failed to create mock row: %v", err))
	}

	return row
}

// Mock Row:
func mockSpannerRowWithInt() *spanner.Row {
	columnNames := []string{"example_int32_column"}
	columnValues := []interface{}{int(12345)}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic(status.Errorf(codes.Internal, "failed to create mock row: %v", err))
	}

	return row
}

func mockSpannerRowWithFloat64() *spanner.Row {
	columnNames := []string{"example_float_column"}
	columnValues := []interface{}{float64(123.45)}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic(status.Errorf(codes.Internal, "failed to create mock row: %v", err))
	}

	return row
}
func mockSpannerRowWithBool() *spanner.Row {
	columnNames := []string{"example_bool_column"}
	columnValues := []interface{}{true}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}
func mockSpannerRowWithBytes() *spanner.Row {
	columnNames := []string{"example_bytes_column"}
	columnValues := []interface{}{[]byte{0x01, 0x02, 0x03, 0x04}}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}
func mockSpannerRowWithDate() *spanner.Row {
	columnNames := []string{"example_date_column"}
	columnValues := []interface{}{spanner.NullDate{Valid: true, Date: civil.Date{Year: 2021, Month: time.January, Day: 1}}}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}
func mockSpannerRowWithTimestamp() *spanner.Row {
	columnNames := []string{"example_timestamp_column"}
	columnValues := []interface{}{time.Date(2021, time.April, 10, 12, 0, 0, 0, time.UTC)}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}
func mockSpannerRowWithIntArray() *spanner.Row {
	columnNames := []string{"test_bigint_array"}
	columnValues := []interface{}{[]int64{1, 2, 3, 4}}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

func mockSpannerRowWithDoubleArray() *spanner.Row {
	columnNames := []string{"test_attributes"}
	columnValues := []interface{}{[]float64{1.5, 2.2, 3.7, 4.2}}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

func mockSpannerRowWithIntArrayForSet() *spanner.Row {
	columnNames := []string{"test_set_bigint"}
	columnValues := []interface{}{[]int64{1, 2, 3, 4}}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}
func mockSpannerRowWithJson() *spanner.Row {
	jsonStr := "{\"math\": true, \"history\": false, \"english\": true}"
	var jsonData interface{}
	err := json.Unmarshal([]byte(jsonStr), &jsonData)
	if err != nil {
		panic(err.Error())
	}
	var jsonString spanner.NullJSON = spanner.NullJSON{
		Value: jsonData,
		Valid: true,
	}
	columnNames := []string{"test_ids"}
	columnValues := []interface{}{jsonString}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}
func mockSpannerRowWithStringArray() *spanner.Row {
	columnNames := []string{"example_string_array_column"}
	columnValues := []interface{}{[]string{"a", "b", "c", "d"}}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

func mockSpannerRowWithTimestampArray() *spanner.Row {
	columnNames := []string{"example_timestamp_array_column"}
	columnValues := []interface{}{[]time.Time{
		time.Date(2023, 1, 15, 12, 30, 0, 0, time.UTC),
		time.Date(2023, 1, 15, 13, 45, 0, 0, time.UTC),
		time.Date(2023, 1, 15, 15, 0, 0, 0, time.UTC),
		time.Date(2023, 1, 15, 16, 15, 0, 0, time.UTC),
	}}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

func mockSpannerRowWithDateArray() *spanner.Row {
	columnNames := []string{"example_date_array_column"}
	columnValues := []interface{}{[]spanner.NullDate{
		{Date: civil.Date{Year: 2023, Month: 1, Day: 15}, Valid: true},
		{Date: civil.Date{Year: 1700, Month: 14, Day: 40}, Valid: false},
	}}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

func mockSpannerRowWithByteArray() *spanner.Row {
	columnNames := []string{"example_byte_array_column"}
	columnValues := []interface{}{[][]byte{
		{0x01, 0x02, 0x03},
		{0x04, 0x05, 0x06, 0x07},
		{0x08, 0x09},
		{0x00},
	}}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

func mockSpannerRowWithBoolArray() *spanner.Row {
	columnNames := []string{"example_bool_array_column"}
	columnValues := []interface{}{[]bool{true, false, true, true}}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

func mockSpannerRowWithString() *spanner.Row {
	columnNames := []string{"test_hash"}
	columnValues := []interface{}{"example test_hash"}
	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}
	return row
}

func mockSpannerRowWithUUIDString() *spanner.Row {
	columnNames := []string{"test_guid"}
	columnValues := []interface{}{"123e4567-e89b-12d3-a456-426614174000"}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

// Mock Struct:
func mockSpannerStructTypeWithInt64() *spannerpb.StructType {
	fields := []*spannerpb.StructType_Field{
		{
			Name: "example_int_column",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_INT64,
			},
		},
	}
	structType := &spannerpb.StructType{
		Fields: fields,
	}

	return structType
}

// Mock Struct:
func mockSpannerStructTypeWithInt() *spannerpb.StructType {
	fields := []*spannerpb.StructType_Field{
		{
			Name: "example_int32_column",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_INT64,
			},
		},
	}
	structType := &spannerpb.StructType{
		Fields: fields,
	}

	return structType
}

func mockSpannerStructTypeWithFloat64() *spannerpb.StructType {
	fields := []*spannerpb.StructType_Field{
		{
			Name: "example_float_column",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_FLOAT64,
			},
		},
	}

	structType := &spannerpb.StructType{
		Fields: fields,
	}

	return structType
}

func mockSpannerStructTypeWithBool() *spannerpb.StructType {
	fields := []*spannerpb.StructType_Field{
		{
			Name: "example_bool_column",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_BOOL,
			},
		},
	}

	structType := &spannerpb.StructType{
		Fields: fields,
	}

	return structType
}
func mockSpannerStructTypeWithDate() *spannerpb.StructType {
	fields := []*spannerpb.StructType_Field{
		{
			Name: "example_date_column",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_DATE,
			},
		},
	}

	structType := &spannerpb.StructType{
		Fields: fields,
	}

	return structType
}
func mockSpannerStructTypeWithBytes() *spannerpb.StructType {
	fields := []*spannerpb.StructType_Field{
		{
			Name: "example_bytes_column",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_BYTES,
			},
		},
	}

	structType := &spannerpb.StructType{
		Fields: fields,
	}

	return structType
}
func mockSpannerStructTypeWithTimestamp() *spannerpb.StructType {
	fields := []*spannerpb.StructType_Field{
		{
			Name: "example_timestamp_column",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_TIMESTAMP,
			},
		},
	}

	structType := &spannerpb.StructType{
		Fields: fields,
	}

	return structType
}
func mockSpannerStructTypeWithIntArray() *spannerpb.StructType {
	fields := []*spannerpb.StructType_Field{
		{
			Name: "test_bigint_array",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_INT64,
				},
			},
		},
	}

	structType := &spannerpb.StructType{
		Fields: fields,
	}

	return structType
}

func mockSpannerStructTypeWithIntArrayForSet() *spannerpb.StructType {
	fields := []*spannerpb.StructType_Field{
		{
			Name: "test_set_bigint",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_INT64,
				},
			},
		},
	}

	structType := &spannerpb.StructType{
		Fields: fields,
	}

	return structType
}
func mockSpannerStructTypeWithJSON() *spannerpb.StructType {
	fields := []*spannerpb.StructType_Field{
		{
			Name: "test_ids",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_JSON,
			},
		},
	}

	structType := &spannerpb.StructType{
		Fields: fields,
	}

	return structType
}

func mockSpannerStructTypeWithString() *spannerpb.StructType {
	fields := []*spannerpb.StructType_Field{
		{
			Name: "test_hash",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_STRING,
			},
		},
	}

	structType := &spannerpb.StructType{
		Fields: fields,
	}

	return structType
}
func mockSpannerStructTypeWithUUIDString() *spannerpb.StructType {
	fields := []*spannerpb.StructType_Field{
		{
			Name: "test_guid",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_STRING,
			},
		},
	}

	structType := &spannerpb.StructType{
		Fields: fields,
	}

	return structType
}

// MetData:
func mockQueryMetadata() responsehandler.QueryMetadata {
	return responsehandler.QueryMetadata{
		Query:        "SELECT * FROM test_table",
		TableName:    "test_table",
		KeyspaceName: "key_space",
		ProtocalV:    4,
	}
}
func mockStringQueryMetadata() responsehandler.QueryMetadata {
	return responsehandler.QueryMetadata{
		Query:        "SELECT * FROM test_table",
		TableName:    "test_table",
		KeyspaceName: "key_space",
		ProtocalV:    4,
	}
}

// Expected Row:
func expectedMessageRowForInt64() message.Row {
	var mockInt64 int64 = 12345
	byteRepresentation := convertInt64ToBytes(mockInt64)
	return message.Row{byteRepresentation}
}

// Expected Row:
func expectedMessageRowForInt() message.Row {
	var mockInt64 int32 = 12345
	byteRepresentation := convertInt32ToBytes(mockInt64)
	return message.Row{byteRepresentation}
}

func expectedMessageRowForFloat64() message.Row {
	var mockFloat64 float64 = 123.45
	byteRepresentation := convertFloat64ToBytes(mockFloat64)
	return message.Row{byteRepresentation}
}
func expectedMessageRowForBytes() message.Row {
	return message.Row{[]byte{0x01, 0x02, 0x03, 0x04}}
}
func expectedMessageRowForBool() message.Row {
	byteRepresentation := convertBoolToBytes(true)
	return message.Row{byteRepresentation}
}
func expectedMessageRowForDate() message.Row {
	byteRepresentation := convertDateToBytes()
	return message.Row{byteRepresentation}
}
func expectedMessageRowForTimestamp() message.Row {
	byteRepresentation := convertTimestampToBytes(time.Date(2021, time.April, 10, 12, 0, 0, 0, time.UTC))
	return message.Row{byteRepresentation}
}
func expectedMessageRowForIntArray() message.Row {
	byteRepresentation := convertIntArrayToBytes([]int64{1, 2, 3, 4})
	return message.Row{byteRepresentation}
}

func expectedMessageRowForJson() message.Row {

	jsonStr := "{\"math\": true, \"history\": false, \"english\": true}"

	var detailsField map[string]bool
	err := json.Unmarshal([]byte(jsonStr), &detailsField)
	if err != nil {
		panic(err.Error())
	}
	mapType := datatype.NewMapType(datatype.Varchar, datatype.Boolean)
	bytes, _ := proxycore.EncodeType(mapType, 4, detailsField)
	return message.Row{bytes}
}

func expectedMessageRowForString() message.Row {
	encodedString := []byte("example test_hash")
	return message.Row{encodedString}
}
func expectedMessageRowForUUID() message.Row {
	encodedUUID := []byte("\x12>Eg\xe8\x9b\x12ӤVBf\x14\x17@\x00")
	return message.Row{encodedUUID}
}

// Expected ColumnMetadata:
func expectedColumnMetadataForInt64() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{
		{
			Keyspace: "key_space",
			Table:    "test_table",
			Name:     "example_int_column",
			Index:    0,
			Type:     datatype.Bigint,
		},
	}
}

// Expected ColumnMetadata:
func expectedColumnMetadataForInt32() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{
		{
			Keyspace: "key_space",
			Table:    "test_table",
			Name:     "example_int32_column",
			Index:    0,
			Type:     datatype.Int,
		},
	}
}
func expectedColumnMetadataForFloat64() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{
		{
			Keyspace: "key_space",
			Table:    "test_table",
			Name:     "example_float_column",
			Index:    0,
			Type:     datatype.Double,
		},
	}
}
func expectedColumnMetadataForBool() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{
		{
			Keyspace: "key_space",
			Table:    "test_table",
			Name:     "example_bool_column",
			Index:    0,
			Type:     datatype.Boolean,
		},
	}
}
func expectedColumnMetadataForBytes() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{
		{
			Keyspace: "key_space",
			Table:    "test_table",
			Name:     "example_bytes_column",
			Index:    0,
			Type:     datatype.Blob,
		},
	}
}
func expectedColumnMetadataForDate() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{
		{
			Keyspace: "key_space",
			Table:    "test_table",
			Name:     "example_date_column",
			Index:    0,
			Type:     datatype.Date,
		},
	}
}
func expectedColumnMetadataForTimestamp() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{
		{
			Keyspace: "key_space",
			Table:    "test_table",
			Name:     "example_timestamp_column",
			Index:    0,
			Type:     datatype.Timestamp,
		},
	}
}
func expectedColumnMetadataForIntArray() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{
		{
			Keyspace: "key_space",
			Table:    "test_table",
			Name:     "test_bigint_array",
			Index:    0,
			Type:     utilities.ListBigintCassandraType,
		},
	}
}
func expectedColumnMetadataForIntArrayForSet() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{
		{
			Keyspace: "key_space",
			Table:    "test_table",
			Name:     "test_set_bigint",
			Index:    0,
			Type:     utilities.SetBigintCassandraType,
		},
	}
}
func expectedColumnMetadataForJson() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{
		{
			Keyspace: "key_space",
			Table:    "test_table",
			Name:     "test_ids",
			Index:    0,
			Type:     datatype.NewMapType(datatype.Varchar, datatype.Boolean),
		},
	}
}

func expectedColumnMetadataForString() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{
		{
			Keyspace: "key_space",
			Table:    "test_table",
			Name:     "test_hash",
			Index:    0,
			Type:     datatype.Varchar,
		},
	}
}
func expectedColumnMetadataForUUID() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{
		{
			Keyspace: "key_space",
			Table:    "test_table",
			Name:     "test_guid",
			Index:    0,
			Type:     datatype.Uuid,
		},
	}
}

// Error
func mockSpannerRowWithError() *spanner.Row {
	columnNames := []string{"example_string_column"}
	columnValues := []interface{}{"invalid data"}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic(fmt.Sprintf("Error creating mock row: %v", err))
	}

	return row
}

func mockSpannerRowWithBoolTypeError() *spanner.Row {
	columnNames := []string{"example_bool_column"}
	columnValues := []interface{}{"not a bool"}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

func mockSpannerRowWithFloat64TypeError() *spanner.Row {
	columnNames := []string{"example_float_column"}
	columnValues := []interface{}{"not a float64"}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

func mockSpannerRowWithBytesTypeError() *spanner.Row {
	columnNames := []string{"example_bytes_column"}
	columnValues := []interface{}{"not bytes"}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

func mockSpannerRowWithDateTypeError() *spanner.Row {
	columnNames := []string{"example_date_column"}
	columnValues := []interface{}{"not a date"}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

func mockSpannerRowWithTimestampTypeError() *spanner.Row {
	columnNames := []string{"example_timestamp_column"}
	columnValues := []interface{}{"not a timestamp"}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

func mockSpannerRowWithStringError() *spanner.Row {
	columnNames := []string{"example_string_column"}
	columnValues := []interface{}{123}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		panic("Failed to create mock spanner.Row: " + err.Error())
	}

	return row
}

// Supported Function:
func convertBoolToBytes(value bool) []byte {
	if value {
		return []byte{1}
	}
	return []byte{0}
}

func convertDateToBytes() []byte {
	var mockDate civil.Date = civil.Date{Year: 2021, Month: time.January, Day: 1}
	t := time.Date(mockDate.Year, mockDate.Month, mockDate.Day, 0, 0, 0, 0, time.UTC)
	byteRepresentation, _ := proxycore.EncodeType(datatype.Date, 4, t)
	return byteRepresentation
}

func convertInt64ToBytes(value int64) []byte {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, value)
	if err != nil {
		panic("Failed to convertInt64ToBytes: " + err.Error())
	}
	return buffer.Bytes()
}

func convertInt32ToBytes(value int32) []byte {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, value)
	if err != nil {
		panic("Failed to convertInt64ToBytes: " + err.Error())
	}
	return buffer.Bytes()
}

func convertFloat64ToBytes(value float64) []byte {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, value)
	if err != nil {
		panic("Failed to convertFloat64ToBytes: " + err.Error())
	}
	return buffer.Bytes()
}

func convertTimestampToBytes(timestamp time.Time) []byte {
	millisSinceEpoch := timestamp.UnixNano() / 1e6
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(millisSinceEpoch))

	return buf
}

func convertIntArrayToBytes(array []int64) []byte {
	bytes, _ := proxycore.EncodeType(utilities.SetBigintCassandraType, 4, array)
	return bytes
}

func convertInt32ArrayToBytes(array []int32) []byte {
	var dt datatype.DataType = datatype.NewListType(datatype.Int)
	bytes, _ := proxycore.EncodeType(dt, 4, array)
	return bytes
}

func convertFloat32ArrayToBytes(array []float32) []byte {
	var dt datatype.DataType = datatype.NewListType(datatype.Float)
	bytes, _ := proxycore.EncodeType(dt, 4, array)
	return bytes
}
func convertStringArrayToBytes(array []string) []byte {
	var dt datatype.DataType = datatype.NewListType(datatype.Varchar)
	bytes, _ := proxycore.EncodeType(dt, 4, array)
	return bytes
}

func convertBoolArrayToBytes(array []bool) []byte {
	var dt datatype.DataType = datatype.NewListType(datatype.Boolean)
	bytes, _ := proxycore.EncodeType(dt, 4, array)
	return bytes
}

func convertFloat64ArrayToBytes(array []float64) []byte {
	var dt datatype.DataType = datatype.NewListType(datatype.Double)
	bytes, _ := proxycore.EncodeType(dt, 4, array)
	return bytes
}

func convertTimestampArrayToBytes(array []time.Time) []byte {
	var dt datatype.DataType = datatype.NewListType(datatype.Timestamp)
	bytes, _ := proxycore.EncodeType(dt, 4, array)
	return bytes
}

func convertByteArrayToBytes(array [][]byte) []byte {
	var dt datatype.DataType = datatype.NewListType(datatype.Blob)
	bytes, _ := proxycore.EncodeType(dt, 4, array)
	return bytes
}

func convertDateArrayToBytes(array []spanner.NullDate, dt datatype.DataType) []byte {
	var encodedArray []int32

	for _, nd := range array {
		if nd.Valid {
			t := responsehandler.ConvertCivilDateToTime(nd.Date)
			cqlDate := responsehandler.ConvertToCQLDate(t)
			encodedArray = append(encodedArray, cqlDate)
		} else {
			defaultDate := responsehandler.ConvertToCQLDate(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))
			encodedArray = append(encodedArray, defaultDate)
		}
	}
	bytes, _ := proxycore.EncodeType(dt, 4, encodedArray)
	return bytes
}
