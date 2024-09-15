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
	"encoding/json"
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/ollionorg/cassandra-to-spanner-proxy/responsehandler"
	"github.com/ollionorg/cassandra-to-spanner-proxy/tableConfig"
	"github.com/ollionorg/cassandra-to-spanner-proxy/utilities"
	"github.com/tj/assert"
	"go.uber.org/zap"
)

func createMockSpannerRow(jsonStr string) *spanner.Row {
	var jsonData interface{}
	err := json.Unmarshal([]byte(jsonStr), &jsonData)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal JSON: %v", err))
	}
	jsonString := spanner.NullJSON{
		Value: jsonData,
		Valid: true,
	}
	columnName := "test_ids"
	row, err := spanner.NewRow([]string{columnName}, []interface{}{jsonString})
	if err != nil {
		panic(fmt.Sprintf("Failed to create mock spanner.Row: %v", err))
	}

	return row
}

func TestTypeHandler_HandleMapType(t *testing.T) {
	th := &responsehandler.TypeHandler{Logger: zap.NewNop()}

	tests := []struct {
		name         string
		dataType     datatype.DataType
		expectedFunc func(int, *spanner.Row) ([]byte, error)
		expectedErr  error
	}{
		{
			name:         "Map of String to Bool",
			dataType:     utilities.MapBooleanCassandraType,
			expectedFunc: th.HandleMapStringBool,
			expectedErr:  nil,
		},
		{
			name:         "Map of String to String",
			dataType:     utilities.MapTextCassandraType,
			expectedFunc: th.HandleMapStringString,
			expectedErr:  nil,
		},
		{
			name:         "Map of String to Bigint",
			dataType:     utilities.MapBigintCassandraType,
			expectedFunc: th.HandleMapStringInt64,
			expectedErr:  nil,
		},
		{
			name:         "Map of String to Double",
			dataType:     utilities.MapDoubleCassandraType,
			expectedFunc: th.HandleMapStringFloat64,
			expectedErr:  nil,
		},
		{
			name:         "Map of String to Date",
			dataType:     utilities.MapDateCassandraType,
			expectedFunc: th.HandleMapStringDate,
			expectedErr:  nil,
		},
		{
			name:         "Map of String to Timestamp",
			dataType:     utilities.MapTimestampCassandraType,
			expectedFunc: th.HandleMapStringTimestamp,
			expectedErr:  nil,
		},
		{
			name:         "Unsupported Map Type",
			dataType:     datatype.NewMapType(datatype.Boolean, datatype.Timestamp),
			expectedFunc: nil,
			expectedErr:  fmt.Errorf("unsupported MAP element type: %v", datatype.NewMapType(datatype.Boolean, datatype.Timestamp)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resultFunc, resultErr := th.HandleMapType(test.dataType)

			if test.expectedErr == nil {
				assert.NoError(t, resultErr)
			} else {
				assert.EqualError(t, resultErr, test.expectedErr.Error())
			}

			assert.Equal(t, fmt.Sprintf("%p", test.expectedFunc), fmt.Sprintf("%p", resultFunc))
		})
	}
}

type MockJsonStringGetter struct {
	JsonStr string
	Err     error
}

func (m *MockJsonStringGetter) GetJsonString(i int, row *spanner.Row) (string, error) {
	return m.JsonStr, m.Err
}

func TestTypeHandler_handleMapType(t *testing.T) {
	mockTableConfig := &tableConfig.TableConfig{
		// Assuming TablesMetaData expects a slice of TableMetaData directly, not a map
		TablesMetaData: map[string]map[string]*tableConfig.Column{
			"test_table": {
				"test_hash": {
					Name:    "test_hash",
					CQLType: "text",
				},
			},
		},
		Logger: zap.NewNop(),
	}
	th := &responsehandler.TypeHandler{TableConfig: mockTableConfig, ProtocalV: primitive.ProtocolVersion4}
	tests := []struct {
		name         string
		elementType  string
		jsonString   string
		wantDataType datatype.DataType
		wantErr      bool
	}{
		{
			name:         "Int64 Map",
			elementType:  "int64",
			jsonString:   `{"key1": 123, "key2": 456}`,
			wantDataType: utilities.MapBigintCassandraType,
			wantErr:      false,
		},
		{
			name:         "String Map",
			elementType:  "string",
			jsonString:   `{"key1": "value1", "key2": "value2"}`,
			wantDataType: utilities.MapTextCassandraType,
			wantErr:      false,
		},
		{
			name:         "Float64 Map",
			elementType:  "float64",
			jsonString:   `{"key1": 1.23, "key2": 4.56}`,
			wantDataType: utilities.MapDoubleCassandraType,
			wantErr:      false,
		},
		{
			name:         "Date Map",
			elementType:  "date",
			jsonString:   `{"key1": "2023-01-01", "key2": "2023-12-31"}`,
			wantDataType: utilities.MapDateCassandraType,
			wantErr:      false,
		},
		{
			name:         "Timestamp Map",
			elementType:  "timestamp",
			jsonString:   `{"key1": "2023-01-01T12:00:00Z", "key2": "2023-12-31T23:59:59Z"}`,
			wantDataType: utilities.MapTimestampCassandraType,
		},
		{
			name:         "Boolean Map",
			elementType:  "boolean",
			jsonString:   `{"key1": true, "key2": false}`,
			wantDataType: utilities.MapBooleanCassandraType,
		},
		{
			name:         "Unsupported Map Type",
			elementType:  "unsupportedType",
			jsonString:   `{"key1": "value1", "key2": "value2"}`,
			wantDataType: datatype.NewMapType(datatype.Bigint, datatype.Timestamp),
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockGetter := &MockJsonStringGetter{
				JsonStr: tt.jsonString,
			}

			th.JsonStringGetter = mockGetter

			// Mock spanner.Row using createMockSpannerRow function
			row := createMockSpannerRow(tt.jsonString)

			rowFunc, err := th.HandleMapType(tt.wantDataType)
			if !tt.wantErr {
				if err != nil {
					t.Errorf("HandleMapType() error = %v, wantErr %v", err, tt.wantErr)
				}
				result, err := rowFunc(0, row)
				if (err != nil) != tt.wantErr {
					t.Errorf("rowFunc() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				if tt.wantErr && result != nil {
					t.Errorf("expected no result, but got %v", result)
				} else if !tt.wantErr && result == nil {
					t.Errorf("expected a valid result, but got nil")
				} else if !tt.wantErr {
					assert.NotEmpty(t, result, "Expected non-empty result")
				}
			} else {
				if err == nil {
					t.Error("expected error but got nil")
				}
			}

		})
	}
}
