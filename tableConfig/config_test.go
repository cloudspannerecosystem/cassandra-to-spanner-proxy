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

package tableConfig

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
)

func TestTableConfig_GetColumnType(t *testing.T) {
	type fields struct {
		Logger         *zap.Logger
		TablesMetaData map[string]map[string]*Column
	}
	type args struct {
		tableName  string
		columnName string
		keyspace   string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *ColumnType
		wantErr bool
	}{
		{
			name: "column exists",
			fields: fields{
				TablesMetaData: map[string]map[string]*Column{
					"table1": {
						"column1": {
							Name:        "column1",
							CQLType:     "text",
							SpannerType: "string",
						},
					},
				},
			},
			args: args{
				tableName:  "table1",
				columnName: "column1",
				keyspace:   "key_space",
			},
			want: &ColumnType{
				CQLType:     "text",
				SpannerType: "string",
			},
			wantErr: false,
		},
		{
			name: "column exists (all caps)",
			fields: fields{
				TablesMetaData: map[string]map[string]*Column{
					"table1": {
						"column1": {
							Name:        "column1",
							CQLType:     "text",
							SpannerType: "string",
						},
					},
				},
			},
			args: args{
				tableName:  "table1",
				columnName: "column1",
				keyspace:   "key_space",
			},
			want: &ColumnType{
				CQLType:     "text",
				SpannerType: "string",
			},
			wantErr: false,
		},
		{
			name: "table does not exist",
			fields: fields{
				TablesMetaData: map[string]map[string]*Column{},
			},
			args: args{
				tableName:  "nonexistent",
				columnName: "column1",
				keyspace:   "key_space",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "column does not exist",
			fields: fields{
				TablesMetaData: map[string]map[string]*Column{
					"table1": {
						"test_hash": {
							Name:        "column2",
							CQLType:     "int",
							SpannerType: "int64",
						},
					},
				},
			},
			args: args{
				tableName:  "table1",
				columnName: "nonexistent",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &TableConfig{
				Logger:         tt.fields.Logger,
				TablesMetaData: tt.fields.TablesMetaData,
			}
			got, err := c.GetColumnType(tt.args.tableName, tt.args.columnName)
			if (err != nil) != tt.wantErr {
				t.Errorf("TableConfig.GetColumnType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TableConfig.GetColumnType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTableConfig_GetColumnMetadata(t *testing.T) {
	mockTableMetadata := map[string]map[string]*Column{
		"testTable": {
			"column1": {
				Metadata: message.ColumnMetadata{Name: "column1", Type: datatype.Varchar},
			},
			"column2": {
				Metadata: message.ColumnMetadata{Name: "column2", Type: datatype.Varchar},
			},
		},
	}
	type fields struct {
		Logger         *zap.Logger
		TablesMetaData map[string]map[string]*Column
	}
	type args struct {
		tableName   string
		columnNames []string
		keyspace    string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*message.ColumnMetadata
		wantErr bool
	}{
		{
			name: "existing table with specific columns",
			fields: fields{
				TablesMetaData: mockTableMetadata,
			},
			args: args{
				tableName:   "testTable",
				columnNames: []string{"column1"},
				keyspace:    "key_space",
			},
			want: []*message.ColumnMetadata{
				{Name: "column1", Type: datatype.Varchar},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &TableConfig{
				Logger:         tt.fields.Logger,
				TablesMetaData: tt.fields.TablesMetaData,
			}
			got, err := c.GetMetadataForColumns(tt.args.tableName, tt.args.columnNames)
			if (err != nil) != tt.wantErr {
				t.Errorf("TableConfig.GetMetadataForColumns() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TableConfig.GetMetadataForColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetPkByTableName(t *testing.T) {
	// Mock TableConfig instance
	tableConfig := &TableConfig{
		PkMetadataCache: map[string][]Column{
			"table1": {
				{Name: "id", IsPrimaryKey: true, PkPrecedence: 1},
			},
			"table2": {
				{Name: "id", IsPrimaryKey: true, PkPrecedence: 1},
				{Name: "description", IsPrimaryKey: true, PkPrecedence: 2},
			},
		},
	}

	tests := []struct {
		name          string
		tableName     string
		expectedPk    []Column
		expectedError error
	}{
		{
			name:       "Valid table name with one primary keys",
			tableName:  "table1",
			expectedPk: []Column{{Name: "id", IsPrimaryKey: true, PkPrecedence: 1}},
		},
		{
			name:      "Valid table name with two primary keys",
			tableName: "table2",
			expectedPk: []Column{
				{Name: "id", IsPrimaryKey: true, PkPrecedence: 1},
				{Name: "description", IsPrimaryKey: true, PkPrecedence: 2},
			},
		},
		{
			name:          "Valid table name without primary keys",
			tableName:     "table3",
			expectedPk:    nil,
			expectedError: fmt.Errorf("could not find table(table3) metadata. Please insert all the table and column information into the config table"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the GetPkByTableName method
			got, err := tableConfig.GetPkByTableName(tt.tableName)

			// Check if the error matches the expected error
			if !errorEquals(err, tt.expectedError) {
				t.Errorf("Expected error: %v, got: %v", tt.expectedError, err)
			}

			// Check if the primary keys match the expected primary keys
			if !reflect.DeepEqual(got, tt.expectedPk) {
				t.Errorf("GetPkByTableName(%v) = %v, want %v", tt.tableName, got, tt.expectedPk)
			}
		})
	}
}

func TestGetColumnMetadata(t *testing.T) {
	// Mock TableConfig instance
	tableConfig := &TableConfig{
		TablesMetaData: map[string]map[string]*Column{
			"table1": {
				"column1": {
					Metadata: message.ColumnMetadata{Name: "column1", Type: datatype.Int, Index: 0},
				}, // Mock column metadata for table1
				"column2": {
					Metadata: message.ColumnMetadata{Name: "column2", Type: datatype.Varchar, Index: 1},
				},
			},
			"table2": {
				"column1": {
					Metadata: message.ColumnMetadata{Name: "column1", Type: datatype.Bigint, Index: 0}, // Mock column metadata for table2
				}, // Mock column metadata for table1
				"column2": {
					Metadata: message.ColumnMetadata{Name: "column2", Type: datatype.Timestamp, Index: 1},
				},
			},
		},
		Logger: zap.NewNop(), // Mock logger
	}

	tests := []struct {
		name             string
		tableName        string
		columnNames      []string
		expectedMetadata []*message.ColumnMetadata
		expectedError    error
	}{
		{
			name:        "Fetch metadata for all columns",
			tableName:   "table1",
			columnNames: nil,
			expectedMetadata: []*message.ColumnMetadata{
				{Name: "column1", Type: datatype.Int, Index: 0},
				{Name: "column2", Type: datatype.Varchar, Index: 1},
			},
			expectedError: nil,
		},
		{
			name:        "Fetch metadata for specific columns",
			tableName:   "table2",
			columnNames: []string{"column1"},
			expectedMetadata: []*message.ColumnMetadata{
				{Name: "column1", Type: datatype.Bigint},
			},
			expectedError: nil,
		},
		{
			name:        "Fetch metadata for ttl_column Name",
			tableName:   "table2",
			columnNames: []string{ttl_column},
			expectedMetadata: []*message.ColumnMetadata{
				{Name: ttl_column, Type: datatype.Int, Index: 0},
			},
			expectedError: nil,
		},
		{
			name:        "Fetch metadata for ts_column Name",
			tableName:   "table2",
			columnNames: []string{ts_column},
			expectedMetadata: []*message.ColumnMetadata{
				{Name: ts_column, Type: datatype.Bigint, Index: 0},
			},
			expectedError: nil,
		},
		{
			name:             "Fetch metadata for non-existent table",
			tableName:        "nonexistent_table",
			columnNames:      nil,
			expectedMetadata: nil,
			expectedError:    errors.New("table nonexistent_table not found"),
		},
		{
			name:             "Fetch metadata for non-existent column",
			tableName:        "table1",
			columnNames:      []string{"nonexistent_column"},
			expectedMetadata: nil,
			expectedError:    errors.New("table = `table1` column name = `nonexistent_column` not found"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Call the GetMetadataForColumns method
			got, err := tableConfig.GetMetadataForColumns(test.tableName, test.columnNames)

			// Check if the error matches the expected error
			if !errorEquals(err, test.expectedError) {
				t.Errorf("Expected error: %v, got: %v", test.expectedError, err)
			}

			// Check if the length of metadata matches the length of expected metadata
			if len(got) != len(test.expectedMetadata) {
				t.Errorf("GetMetadataForColumns(%v) = %v, want %v", test.tableName, len(got), len(test.expectedMetadata))
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

var mockTableMetadata = map[string]map[string]*Column{
	"testTable": {
		"column1": {
			Metadata: message.ColumnMetadata{Name: "column1", Type: datatype.Varchar},
		},
		"column2": {
			Metadata: message.ColumnMetadata{Name: "column2", Type: datatype.Varchar},
		},
	},
}

func TestTableConfig_GetMetadataForSelectedColumns(t *testing.T) {
	type fields struct {
		Logger         *zap.Logger
		TablesMetaData map[string]map[string]*Column
	}
	type args struct {
		tableName   string
		columnNames []SelectedColumns
		aliasMap    map[string]AsKeywordMeta
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*message.ColumnMetadata
		wantErr bool
	}{
		{
			name: "existing table with specific columns",
			fields: fields{
				Logger:         zap.NewNop(),
				TablesMetaData: mockTableMetadata,
			},
			args: args{
				tableName: "testTable",
				columnNames: []SelectedColumns{
					{Name: "column1"},
				},
				aliasMap: map[string]AsKeywordMeta{},
			},
			want: []*message.ColumnMetadata{
				{Name: "column1", Type: datatype.Varchar},
			},
			wantErr: false,
		},
		{
			name: "special table with function ",
			fields: fields{
				Logger:         zap.NewNop(),
				TablesMetaData: mockTableMetadata,
			},
			args: args{
				tableName: "testTable",
				columnNames: []SelectedColumns{
					{Name: "column1", IsFunc: true, FuncName: "count", IsAs: true, Alias: "count_x"},
				},
				aliasMap: map[string]AsKeywordMeta{"count_x": AsKeywordMeta{IsFunc: true, Name: "count", Alias: "count_x", CQLType: "bigint", DataType: datatype.Bigint}},
			},
			want: []*message.ColumnMetadata{
				{Name: "count_x", Type: datatype.Bigint},
			},
			wantErr: false,
		},
		{
			name: "table not found",
			fields: fields{
				Logger:         zap.NewNop(),
				TablesMetaData: mockTableMetadata,
			},
			args: args{
				tableName: "nonExistentTable",
				columnNames: []SelectedColumns{
					{Name: "column1"},
				},
				aliasMap: map[string]AsKeywordMeta{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "column not found",
			fields: fields{
				Logger:         zap.NewNop(),
				TablesMetaData: mockTableMetadata,
			},
			args: args{
				tableName: "testTable",
				columnNames: []SelectedColumns{
					{Name: "nonExistentColumn"},
				},
				aliasMap: map[string]AsKeywordMeta{},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &TableConfig{
				Logger:         tt.fields.Logger,
				TablesMetaData: tt.fields.TablesMetaData,
			}
			got, err := c.GetMetadataForSelectedColumns(tt.args.tableName, tt.args.columnNames, tt.args.aliasMap)
			if (err != nil) != tt.wantErr {
				t.Errorf("TableConfig.GetMetadataForSelectedColumns() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TableConfig.GetMetadataForSelectedColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}
