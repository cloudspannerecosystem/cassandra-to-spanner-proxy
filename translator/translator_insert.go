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
	"strconv"
	"strings"
	"time"

	"errors"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	cql "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/translator/cqlparser"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/utilities"
)

// parseColumnsAndValuesFromInsert parses columns and values from the Insert query
//
// Parameters:
//   - input: Insert Column Spec Context from antlr parser.
//   - tableName: Table Name
//   - tableConfig: JSON Config which maintains column and its datatypes info.
//
// Returns: ColumnsResponse struct and error if any
func parseColumnsAndValuesFromInsert(input cql.IInsertColumnSpecContext, tableName string, tableConfig *tableConfig.TableConfig) (*ColumnsResponse, error) {
	if input == nil {
		return nil, errors.New("parseColumnsAndValuesFromInsert: No Input parameters found for columns")
	}
	columnListObj := input.ColumnList()
	if columnListObj == nil {
		return nil, errors.New("parseColumnsAndValuesFromInsert: error while parsing columns")
	}

	columns := columnListObj.AllColumn()
	if len(columns) == 0 {
		return nil, errors.New("parseColumnsAndValuesFromInsert: No Columns found in the Insert Query")
	}

	var columnArr []Column
	var valuesArr []string
	var paramKeys []string
	var primaryColumns []string

	for _, val := range columns {
		columnName := strings.ToLower(val.GetText())
		if columnName == "" {
			return nil, errors.New("parseColumnsAndValuesFromInsert: No Columns found in the Insert Query")
		}
		columnName = strings.ReplaceAll(columnName, literalPlaceholder, "")
		columnType, err := tableConfig.GetColumnType(tableName, columnName)
		if err != nil {
			return nil, errors.New("parseColumnsAndValuesFromInsert: No Column type found in the Table Config")
		}
		column := Column{
			Name:        columnName,
			SpannerType: columnType.SpannerType,
			CQLType:     columnType.CQLType,
		}
		valueName := "@" + columnName
		columnArr = append(columnArr, column)
		valuesArr = append(valuesArr, valueName)
		paramKeys = append(paramKeys, columnName)
		if columnType.IsPrimaryKey {
			primaryColumns = append(primaryColumns, columnName)
		}
	}

	response := &ColumnsResponse{
		Columns:       columnArr,
		ValueArr:      valuesArr,
		ParamKeys:     paramKeys,
		PrimayColumns: primaryColumns,
	}
	return response, nil

}

// setParamsFromValues parses Values from the Insert Query
//
// Parameters:
//   - input: Insert Value Spec Context from antlr parser.
//   - columns: Array of Column Names
//
// Returns: Map Interface for param name as key and its value and error if any
func setParamsFromValues(input cql.IInsertValuesSpecContext, columns []Column) (map[string]interface{}, []interface{}, error) {
	if input == nil {
		return nil, nil, errors.New("setParamsFromValues: No Value parameters found")
	}

	valuesExpressionList := input.ExpressionList()
	if valuesExpressionList == nil {
		return nil, nil, errors.New("setParamsFromValues: error while parsing values")
	}

	values := valuesExpressionList.AllExpression()
	if values == nil {
		return nil, nil, errors.New("setParamsFromValues: error while parsing values")
	}
	var valuesArr []string
	var respValue []interface{}

	for _, val := range values {
		valueName := val.GetText()
		if valueName != "" {
			valuesArr = append(valuesArr, valueName)
		} else {
			return nil, nil, errors.New("setParamsFromValues: Invalid Value parameters")
		}
	}

	if len(valuesArr) != len(columns) {
		return nil, nil, errors.New("setParamsFromValues: mismatch between columns and values")
	}

	response := make(map[string]interface{})

	for i, col := range columns {
		value := valuesArr[i]
		if value != questionMark {
			colName := col.Name
			value = strings.ReplaceAll(value, "'", "")
			val, err := formatValues(value, col.SpannerType, col.CQLType)
			if err != nil {
				return nil, nil, err
			}
			response[colName] = val
			respValue = append(respValue, val)
		}
	}

	return response, respValue, nil
}

// getTTLValue parses Value for Using TTL
//
// Parameters:
//   - ttlTsSpec: Using TTL TImestamp Spec Context from antlr parser.
//
// Returns: string as timestamp value and error if any
func getTTLValue(ttlTsSpec cql.IUsingTtlTimestampContext) (string, error) {
	if ttlTsSpec == nil {
		return "", errors.New("invalid input")
	}
	ttlTsSpec.KwUsing()
	ttlSpec := ttlTsSpec.Ttl()

	if ttlSpec == nil {
		return questionMark, nil
	}
	ttlSpec.KwTtl()
	valLiteral := ttlSpec.DecimalLiteral()
	var resp string

	if valLiteral == nil {
		return "", errors.New("no value found for TTL")
	}

	resp = valLiteral.GetText()
	resp = strings.ReplaceAll(resp, "'", "")
	return resp, nil
}

// getTTLValue parses Value for USING TIMESTAMP
//
// Parameters:
//   - spec: Using TTL TImestamp Spec Context from antlr parser.
//
// Returns: string as timestamp value and error if any
func getTimestampValue(spec cql.IUsingTtlTimestampContext) (string, error) {
	if spec == nil {
		return "", errors.New("invalid input")
	}

	spec.KwUsing()
	tsSpec := spec.Timestamp()

	if tsSpec == nil {
		return questionMark, nil
	}
	tsSpec.KwTimestamp()
	valLiteral := tsSpec.DecimalLiteral()
	var resp string

	if valLiteral == nil {
		return "", errors.New("no value found for Timestamp")
	}

	resp = valLiteral.GetText()
	resp = strings.ReplaceAll(resp, "'", "")
	return resp, nil
}

// formatTSCheckQuery prepares the SELECT query to check for USING TIMESTAMP
//
// Parameters:
//   - primaryCols: List of Columns which are Primary key
//   - tableName:   table name
//
// Returns: string as select query value and error if any
func formatTSCheckQuery(primaryCols []string, tableName string) (string, error) {
	if len(primaryCols) == 0 {
		return "", errors.New("function needs atleast 1 primary key")
	}

	if tableName == "" {
		return "", errors.New("table name is empty")
	}
	clauseStr := ""
	queryStr := ""
	tsColumn := "`" + spannerTSColumn + "`"
	tsValuePlaceholder := "@" + spannerTSColumn

	for _, val := range primaryCols {
		valPlaceholder := "@" + val
		column := "`" + val + "`"
		if clauseStr == "" {
			clauseStr = clauseStr + " " + column + " " + "=" + " " + valPlaceholder
		} else {
			clauseStr = clauseStr + " AND " + column + " " + "=" + " " + valPlaceholder
		}
	}

	clauseStr = clauseStr + " AND " + tsColumn + " " + ">" + " " + tsValuePlaceholder

	queryStr = "SELECT " + spannerTSColumn + " FROM " + tableName + " WHERE" + clauseStr + ";"

	return queryStr, nil
}

// getSpannerInsertQuery frames insert query for spanner.
//
// Parameters:
//   - data: InsertQueryMap struct
//
// Returns: Spanner Insert query and error if any
func getSpannerInsertQuery(data *InsertQueryMap, useRowTimestamp bool) (string, error) {
	var columnNames []string
	var valuePlaceholders []string

	if data.Table != "" && len(data.ParamKeys) > 0 {

		for _, val := range data.ParamKeys {
			columnNames = append(columnNames, "`"+val+"`")
			if val == spannerTSColumn && data.UsingTSCheck == "" {
				valuePlaceholders = append(valuePlaceholders, commitTsFn)
			} else {
				valuePlaceholders = append(valuePlaceholders, "@"+val)
			}
		}

		if len(data.Values) == 0 && data.UsingTSCheck == "" && useRowTimestamp {
			columnNames = append(columnNames, "`"+spannerTSColumn+"`")
			valuePlaceholders = append(valuePlaceholders, commitTsFn)
		}

		columnStr := "(" + strings.Join(columnNames, ", ") + ")"
		valueStr := "(" + strings.Join(valuePlaceholders, ", ") + ")"

		spannerQuery := "INSERT OR UPDATE INTO " + data.Table + " " + columnStr + " " + "VALUES" + " " + valueStr + ";"

		return spannerQuery, nil
	}
	return "", errors.New("one or more details not found to create Spanner query")
}

// ToSpannerUpsert frames Spanner supported query for Insert Query
//
// Parameters:
//   - query: CQL Insert query
//
// Returns: InsertQueryMap struct and error if any
func (t *Translator) ToSpannerUpsert(keyspace string, queryStr string) (*InsertQueryMap, error) {
	lowerQuery := strings.ToLower(queryStr)
	query := renameLiterals(queryStr)
	p, err := NewCqlParser(query, t.Debug)
	if err != nil {
		return nil, err
	}

	insertObj := p.Insert()
	if insertObj == nil {
		return nil, errors.New("could not parse insert object")
	}
	kwInsertObj := insertObj.KwInsert()
	if kwInsertObj == nil {
		return nil, errors.New("could not parse insert object")
	}
	queryType := kwInsertObj.GetText()
	insertObj.KwInto()
	table := insertObj.Table()

	var columnsResponse ColumnsResponse
	var checkTSQuery string
	var formattedTS *time.Time
	var keyspaceName string

	if insertObj.Keyspace() != nil {
		keyspaceObj := insertObj.Keyspace().OBJECT_NAME()
		if keyspaceObj == nil {
			return nil, errors.New("invalid input parameter found for keyspace")
		}
		keyspaceName = strings.ToLower(keyspaceObj.GetText())
	} else if len(keyspace) != 0 {
		keyspaceName = RemoveQuotesFromKeyspace(keyspace)
	} else {
		return nil, errors.New("invalid input parameter found for keyspace")
	}

	if table == nil {
		return nil, errors.New("invalid input parameter found for table")
	}
	tableobj := table.OBJECT_NAME()
	if tableobj == nil {
		return nil, errors.New("invalid input parameter found for table")
	}
	tableName := strings.ToLower(tableobj.GetText())
	if tableName != "" && strings.Contains(tableName, "missing") {
		return nil, errors.New("no input parameter found for table")
	}
	if keyspaceName == "" {
		return nil, errors.New("keyspace is null")
	}

	tableName = utilities.FlattenTableName(t.KeyspaceFlatter, keyspaceName, tableName)

	resp, err := parseColumnsAndValuesFromInsert(insertObj.InsertColumnSpec(), tableName, t.TableConfig)
	if err != nil {
		return nil, err
	}

	if resp != nil {
		columnsResponse = *resp
	}
	params, values, err := setParamsFromValues(insertObj.InsertValuesSpec(), columnsResponse.Columns)
	if err != nil {
		return nil, err
	}

	hasUsingTimestamp := hasUsingTimestamp(lowerQuery)
	hasIfNotExists := hasIfNotExists(lowerQuery)

	if hasUsingTimestamp && !t.UseRowTimestamp {
		return nil, errors.New("'USING TIMESTAMP' is not enabled. Set 'useRowTimestamp' to true in config.yaml")
	}

	if hasIfNotExists && hasUsingTimestamp {
		return nil, errors.New("INSERT does not support IF NOT EXISTS and USING TIMESTAMP in the same statement")
	}

	hasUsingTtl := hasUsingTtl(lowerQuery)
	if hasUsingTtl && !t.UseRowTTL {
		return nil, errors.New("'USING TTL' is not enabled. Set 'useRowTTL' to true in config.yaml")
	}
	if hasUsingTimestamp || hasUsingTtl {

		ttlTsSpec := insertObj.UsingTtlTimestamp()
		if ttlTsSpec == nil {
			return nil, errors.New("error parsing ttl timestamp spec")
		}
		var tsValue string

		if hasUsingTimestamp {
			tsValue, err = getTimestampValue(ttlTsSpec)
			if err != nil {
				return nil, err
			}

			if tsValue != "" && tsValue != questionMark {
				ts64, err := strconv.ParseInt(tsValue, 10, 64)
				if err != nil {
					return nil, err
				}

				formattedTS, err = utilities.FormatTimestamp(ts64)
				if err != nil {
					return nil, err
				}

				tsValue = formattedTS.UTC().String()

			}

			checkTSQuery, err = formatTSCheckQuery(resp.PrimayColumns, tableName)
			if err != nil {
				return nil, err
			}

			if tsValue != "" && tsValue != questionMark {
				params[spannerTSColumn] = formattedTS
			}
		} else {
			if len(values) > 0 {
				columnsResponse.ParamKeys = append(columnsResponse.ParamKeys, spannerTSColumn)
				values = append(values, spanner.CommitTimestamp)
			}
		}

		if hasUsingTtl {
			ttlValue, err := getTTLValue(ttlTsSpec)
			if err != nil {
				return nil, err
			}

			columnsResponse.Columns = append(columnsResponse.Columns, Column{
				Name:        spannerTTLColumn,
				SpannerType: "int",
				CQLType:     "int64",
			})

			if ttlValue != "" && ttlValue != questionMark {
				castedToInt64, err := strconv.ParseInt(ttlValue, 10, 64)
				if err != nil {
					return nil, err
				}

				ttlValue = utilities.AddSecondsToCurrentTimestamp(castedToInt64)
				params[ttlValuePlaceholder] = ttlValue
			}

			if tsValue != "" {
				if isTTLFirst(lowerQuery) {
					columnsResponse.ParamKeys = append(columnsResponse.ParamKeys, spannerTTLColumn)
					columnsResponse.ParamKeys = append(columnsResponse.ParamKeys, spannerTSColumn)
					if ttlValue != "" && ttlValue != questionMark {
						values = append(values, ttlValue)

					}
					if tsValue != "" && tsValue != questionMark {
						values = append(values, formattedTS)
					}
				} else {
					columnsResponse.ParamKeys = append(columnsResponse.ParamKeys, spannerTSColumn)
					columnsResponse.ParamKeys = append(columnsResponse.ParamKeys, spannerTTLColumn)
					if tsValue != "" && tsValue != questionMark {
						values = append(values, formattedTS)
					}
					if ttlValue != "" && ttlValue != questionMark {
						values = append(values, ttlValue)

					}
				}
			} else {
				columnsResponse.ParamKeys = append(columnsResponse.ParamKeys, spannerTTLColumn)
				if ttlValue != "" && ttlValue != questionMark {
					values = append(values, ttlValue)
				}
			}
		} else {
			if tsValue != "" {
				columnsResponse.ParamKeys = append(columnsResponse.ParamKeys, spannerTSColumn)
				if tsValue != questionMark {
					values = append(values, formattedTS)
				}
			}
		}
	} else if t.UseRowTimestamp {
		if len(values) > 0 {
			columnsResponse.ParamKeys = append(columnsResponse.ParamKeys, spannerTSColumn)
			values = append(values, spanner.CommitTimestamp)
		}
	}

	primaryKeys, err := getPrimaryKeys(t.TableConfig, tableName)
	if err != nil {
		return nil, err
	}

	insertQueryData := &InsertQueryMap{
		CassandraQuery: query,
		QueryType:      queryType,
		Table:          tableName,
		Keyspace:       keyspaceName,
		Columns:        columnsResponse.Columns,
		Values:         values,
		SpannerQuery:   "",
		Params:         params,
		ParamKeys:      columnsResponse.ParamKeys,
		UsingTSCheck:   checkTSQuery,
		HasIfNotExists: hasIfNotExists,
		PrimaryKeys:    primaryKeys,
	}

	spannerQuery, err := getSpannerInsertQuery(insertQueryData, t.UseRowTimestamp)
	if err != nil {
		return nil, err
	}

	insertQueryData.SpannerQuery = spannerQuery

	return insertQueryData, nil

}
