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
	"fmt"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
)

const (
	ttl_column = "spanner_ttl_ts"
	ts_column  = "last_commit_ts"
	limitValue = "limitValue"
)

type Column struct {
	Name         string
	CQLType      string
	SpannerType  string
	IsPrimaryKey bool
	PkPrecedence int64
	Metadata     message.ColumnMetadata
}

// Select Query Models
type AsKeywordMeta struct {
	IsFunc   bool
	Name     string
	Alias    string
	DataType datatype.DataType
	CQLType  string
}

type TableConfig struct {
	Logger          *zap.Logger
	TablesMetaData  map[string]map[string]*Column
	PkMetadataCache map[string][]Column
	KeyspaceFlatter bool
}

type ColumnType struct {
	CQLType      string
	SpannerType  string
	IsPrimaryKey bool
}

type SelectedColumns struct {
	FormattedColumn string
	Name            string
	IsFunc          bool
	IsAs            bool
	FuncName        string
	Alias           string
}

// GetPkByTableName searches for and retrieves the primary keys of a specific table
//
// Parameters:
//   - tableName: A string representing the name of the table for which the metadata
//     is requested.
//
// Returns:
//   - []Column: Array of Column details which are Primary Keys.
//   - An error: Returns error if function not able to search any primary keys
func (c *TableConfig) GetPkByTableName(tableName string) ([]Column, error) {
	pkMeta, ok := c.PkMetadataCache[tableName]

	if !ok {
		return nil, fmt.Errorf("could not find table(%s) metadata. Please insert all the table and column information into the config table", tableName)
	}

	return pkMeta, nil
}

// GetColumnType retrieves the column types for a specified column in a specified table.
// This method is a part of the TableConfig struct and is used to map column types
// from Cassandra (CQL) to Google Cloud Spanner.
//
// Parameters:
// - tableName: A string representing the name of the table.
// - columnName: A string representing the name of the column within the specified table.
//
// Returns:
//   - A pointer to a ColumnType struct that contains both the CQL type and the corresponding
//     Spanner type for the specified column.
//   - An error if the function is unable to determine the corresponding Spanner type.
func (c *TableConfig) GetColumnType(tableName string, columnName string) (*ColumnType, error) {
	td, ok := c.TablesMetaData[tableName]
	if !ok {
		return nil, fmt.Errorf("could not find table(%s) metadata. Please insert all the table and column information into the config table", tableName)
	}

	col, ok := td[columnName]
	if !ok {
		return nil, fmt.Errorf("could not find column(%s) metadata. Please insert all the table and column information into the config table", columnName)
	}

	return &ColumnType{
		CQLType:      col.CQLType,
		SpannerType:  col.SpannerType,
		IsPrimaryKey: col.IsPrimaryKey,
	}, nil
}

// GetMetadataForColumns retrieves metadata for specific columns in a given table.
// This method is a part of the TableConfig struct.
//
// Parameters:
//   - tableName: The name of the table for which column metadata is being requested.
//   - columnNames(optional):Accepts nil if no columnNames provided or else A slice of strings containing the names of the columns for which
//     metadata is required. If this slice is empty, metadata for all
//     columns in the table will be returned.
//
// Returns:
// - A slice of pointers to ColumnMetadata structs containing metadata for each requested column.
// - An error if the specified table is not found in the TablesMetaData.
func (c *TableConfig) GetMetadataForColumns(tableName string, columnNames []string) ([]*message.ColumnMetadata, error) {
	columnsMap, ok := c.TablesMetaData[tableName]
	if !ok {
		c.Logger.Error("Table not found in Column MetaData Cache")
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	if len(columnNames) == 0 {
		return c.getAllColumnsMetadata(columnsMap), nil
	}

	return c.getSpecificColumnsMetadata(columnsMap, columnNames, tableName)
}

// GetMetadataForSelectedColumns simply fetching column metadata for selected columns .
//
// Parameters:
//   - tableName: The name of the table for which column metadata is being requested.
//   - columnNames(optional): []SelectedColumns - Accepts nil if no columnNames provided or else A slice of strings containing the names of the columns for which
//     metadata is required. If this slice is empty, metadata for all
//     columns in the table will be returned.
//
// Returns:
// - A slice of pointers to ColumnMetadata structs containing metadata for each requested column.
// - An error if the specified table is not found in the TablesMetaData.
func (c *TableConfig) GetMetadataForSelectedColumns(tableName string, columnNames []SelectedColumns, aliasMap map[string]AsKeywordMeta) ([]*message.ColumnMetadata, error) {
	columnsMap, ok := c.TablesMetaData[tableName]
	if !ok {
		c.Logger.Error("Table not found in Column MetaData Cache")
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	if len(columnNames) == 0 {
		return c.getAllColumnsMetadata(columnsMap), nil
	}
	return c.getSpecificColumnsMetadataForSelectedColumns(columnsMap, columnNames, tableName, aliasMap)
}

func (c *TableConfig) getSpecificColumnsMetadataForSelectedColumns(columnsMap map[string]*Column, selectedColumns []SelectedColumns, tableName string, aliasMap map[string]AsKeywordMeta) ([]*message.ColumnMetadata, error) {
	var columnMetadataList []*message.ColumnMetadata
	var columnName string
	for i, columnMeta := range selectedColumns {
		columnName = columnMeta.Name
		if columnMeta.IsAs || columnMeta.IsFunc {
			if aliasInfo, ok := aliasMap[columnMeta.Alias]; ok {
				for _, column := range columnsMap {
					columnMetaData := c.cloneColumnMetadata(&column.Metadata, int32(i))
					columnMetaData.Name = columnMeta.Alias
					columnMetaData.Type = aliasInfo.DataType
					columnMetadataList = append(columnMetadataList, columnMetaData)
					break
				}
			}

		} else if column, ok := columnsMap[columnName]; ok {
			columnMetadataList = append(columnMetadataList, c.cloneColumnMetadata(&column.Metadata, int32(i)))
		} else if isSpecialColumn(columnName) {
			metadata, err := c.handleSpecialColumn(columnsMap, columnName, int32(i))
			if err != nil {
				return nil, err
			}
			columnMetadataList = append(columnMetadataList, metadata)
		} else if columnMeta.IsFunc {
			c.Logger.Debug("Identified a function call", zap.String("columnName", columnName))
		} else {
			err := fmt.Errorf("table = `%s` column name = `%s` not found", tableName, columnName)
			c.Logger.Error(err.Error())
			return nil, err
		}
	}
	return columnMetadataList, nil
}

// getAllColumnsMetadata retrieves metadata for all columns in a given table.
//
// Parameters:
//   - columnsMap: column info for a given table.
//
// Returns:
// - A slice of pointers to ColumnMetadata structs containing metadata for each requested column.
// - An error
func (c *TableConfig) getAllColumnsMetadata(columnsMap map[string]*Column) []*message.ColumnMetadata {
	var columnMetadataList []*message.ColumnMetadata
	var i int32 = 0
	for _, column := range columnsMap {
		columnMt := column.Metadata
		columnMd := columnMt.Clone()
		columnMd.Index = i
		columnMetadataList = append(columnMetadataList, columnMd)
		i++
	}
	return columnMetadataList
}

// getSpecificColumnsMetadata retrieves metadata for specific columns in a given table.
//
// Parameters:
//   - columnsMap: column info for a given table.
//   - columnNames: column names for which the metadata is required.
//   - tableName: name of the table
//
// Returns:
// - A slice of pointers to ColumnMetadata structs containing metadata for each requested column.
// - An error
func (c *TableConfig) getSpecificColumnsMetadata(columnsMap map[string]*Column, columnNames []string, tableName string) ([]*message.ColumnMetadata, error) {
	var columnMetadataList []*message.ColumnMetadata
	for i, columnName := range columnNames {
		if column, ok := columnsMap[columnName]; ok {
			columnMetadataList = append(columnMetadataList, c.cloneColumnMetadata(&column.Metadata, int32(i)))
		} else if isSpecialColumn(columnName) {
			metadata, err := c.handleSpecialColumn(columnsMap, columnName, int32(i))
			if err != nil {
				return nil, err
			}
			columnMetadataList = append(columnMetadataList, metadata)
		} else {
			err := fmt.Errorf("table = `%s` column name = `%s` not found", tableName, columnName)
			c.Logger.Error(err.Error())
			return nil, err
		}
	}
	return columnMetadataList, nil
}

// handleSpecialColumn retrieves metadata for special columns.
//
// Parameters:
//   - columnsMap: column info for a given table.
//   - columnName: column name for which the metadata is required.
//   - index: Index for the column
//
// Returns:
// - Pointers to ColumnMetadata structs containing metadata for each requested column.
// - An error
func (c *TableConfig) handleSpecialColumn(columnsMap map[string]*Column, columnName string, index int32) (*message.ColumnMetadata, error) {
	var expectedType datatype.DataType
	if columnName == ttl_column || columnName == limitValue {
		expectedType = datatype.Int
	} else if columnName == ts_column {
		expectedType = datatype.Bigint
	}
	for _, column := range columnsMap {
		columnMd := column.Metadata.Clone()
		columnMd.Index = index
		columnMd.Name = columnName
		columnMd.Type = expectedType
		return columnMd, nil
	}
	return nil, fmt.Errorf("special column %s not found", columnName)
}

// cloneColumnMetadata clones the metadata from cache.
//
// Parameters:
//   - metadata: Column metadata from cache
//   - index: Index for the column
//
// Returns:
// - Pointers to ColumnMetadata structs containing metadata for each requested column.
func (c *TableConfig) cloneColumnMetadata(metadata *message.ColumnMetadata, index int32) *message.ColumnMetadata {
	columnMd := metadata.Clone()
	columnMd.Index = index
	return columnMd
}

// isSpecialColumn to check if its a special column.
//
// Parameters:
//   - columnName: name of special column
//
// Returns:
// - boolean
func isSpecialColumn(columnName string) bool {
	return columnName == ttl_column || columnName == limitValue || columnName == ts_column
}
