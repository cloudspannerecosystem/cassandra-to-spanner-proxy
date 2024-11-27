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
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	cql "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/translator/cqlparser"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
)

const excludeInternalColumns = " EXCEPT (`" + spannerTSColumn + "`, `" + spannerTTLColumn + "`)"
const includeTSColumn = " EXCEPT (`" + spannerTSColumn + "`)"
const includeTTLColumn = " EXCEPT (`" + spannerTTLColumn + "`)"

// parseColumnsFromSelect parse Columns from the Select Query
//
// Parameters:
//   - input: The Select Element context from the antlr Parser.
//
// Returns: Column Meta and an error if any.
func parseColumnsFromSelect(input cql.ISelectElementsContext) (ColumnMeta, error) {
	var response ColumnMeta
	var funcName, argument string
	if input == nil {
		return response, errors.New("no Input parameters found for columns")
	}

	if input.STAR() != nil {
		response.Star = true
	} else {
		columns := input.AllSelectElement()

		if len(columns) == 0 {
			return response, errors.New("no column parameters found in the query")
		}

		for _, val := range columns {
			var selectedColumns tableConfig.SelectedColumns
			if val == nil {
				return response, errors.New("error while parsing the values")
			}

			funcCall := val.FunctionCall()
			if funcCall == nil {
				selectedColumns.Name = strings.ToLower(val.GetText())
			} else {
				selectedColumns.IsFunc = true
				if funcCall.OBJECT_NAME() == nil {
					return response, errors.New("function call object is nil")
				}
				funcName = strings.ToLower(funcCall.OBJECT_NAME().GetText())

				if funcCall.STAR() != nil {
					argument = strings.ToLower(funcCall.STAR().GetText())
					selectedColumns.Alias = funcName
				} else {
					if funcCall.FunctionArgs() == nil {
						return response, errors.New("function call argument object is nil")
					}
					argument = strings.ToLower(funcCall.FunctionArgs().GetText())
					selectedColumns.Alias = funcName + "_" + argument
				}
				selectedColumns.Name = argument
				selectedColumns.FuncName = funcName
			}
			if strings.Contains(selectedColumns.Name, missingUndefined) {
				return response, errors.New("one or more undefined fields for column name")
			}
			asOperator := val.KwAs()
			if asOperator != nil {
				selectedColumns.IsAs = true
				allObject := val.AllOBJECT_NAME()
				if len(allObject) == 2 {
					selectedColumns.Name = strings.ToLower(allObject[0].GetText())
					selectedColumns.Alias = strings.ToLower(allObject[1].GetText())
				} else if len(allObject) == 1 {
					selectedColumns.Alias = strings.ToLower(val.OBJECT_NAME(0).GetText())
				} else {
					return response, errors.New("unknown flow with as operator")
				}
			}
			selectedColumns.Name = strings.ReplaceAll(selectedColumns.Name, literalPlaceholder, "")
			response.Column = append(response.Column, selectedColumns)
		}
	}

	return response, nil

}

// parseTableFromSelect parse Table Name from the Select Query
//
// Parameters:
//   - input: The From Spec context from the antlr Parser.
//
// Returns: Table Name and an error if any.
func parseTableFromSelect(keyspace string, input cql.IFromSpecContext) (*TableObj, error) {
	if input == nil {
		return nil, errors.New("no input parameters found for table and keyspace")
	}

	var response TableObj
	fromSpec := input.FromSpecElement()
	if fromSpec == nil {
		return nil, errors.New("error while parsing fromSpec")
	}
	allObj := fromSpec.AllOBJECT_NAME()
	if allObj == nil {
		return nil, errors.New("error while parsing all objects from the fromSpec")
	}

	if len(allObj) == 0 {
		return nil, errors.New("could not find table and keyspace name")
	}

	var tableObj antlr.TerminalNode
	var keyspaceObj antlr.TerminalNode
	if len(allObj) == 2 {
		keyspaceObj = fromSpec.OBJECT_NAME(0)
		tableObj = fromSpec.OBJECT_NAME(1)
	} else {
		tableObj = fromSpec.OBJECT_NAME(0)
	}

	if tableObj != nil {
		response.TableName = strings.ToLower(tableObj.GetText())
	} else {
		return nil, errors.New("could not find table name")
	}

	if keyspaceObj != nil {
		response.KeyspaceName = strings.ToLower(keyspaceObj.GetText())
	} else if len(keyspace) != 0 {
		response.KeyspaceName = RemoveQuotesFromKeyspace(keyspace)
	} else {
		return nil, errors.New("could not find keyspace name")
	}
	return &response, nil
}

// parseOrderByFromSelect parse Order By from the Select Query
//
// Parameters:
//   - input: The Order Spec context from the antlr Parser.
//
// Returns: OrderBy struct
func parseOrderByFromSelect(input cql.IOrderSpecContext) OrderBy {
	var response OrderBy

	if input == nil {
		response.isOrderBy = false
		return response
	}
	response.isOrderBy = true
	colName := input.OrderSpecElement().OBJECT_NAME().GetText()
	if strings.Contains(colName, missingUndefined) {
		response.isOrderBy = false
		return response
	}
	colName = strings.ReplaceAll(colName, literalPlaceholder, "")
	response.Column = "`" + colName + "`"

	response.Operation = "asc"
	if input.OrderSpecElement().KwDesc() != nil {
		response.Operation = "desc"
	}

	return response
}

// parseLimitFromSelect parse Limit from the Select Query
//
// Parameters:
//   - input: The Limit Spec context from the antlr Parser.
//
// Returns: Limit struct
func parseLimitFromSelect(input cql.ILimitSpecContext) Limit {
	var response Limit

	if input == nil {
		return response
	}

	count := input.DecimalLiteral().GetText()
	if count == "" || strings.Contains(count, missingUndefined) {
		return response
	}

	response.isLimit = true
	response.Count = count
	return response
}

// processStrings processes the selected columns, formats them, and returns a map of aliases to metadata and a slice of formatted columns.
// Parameters:
//   - t : Translator instance
//   - selectedColumns: []tableConfig.SelectedColumns
//   - tableName : table name on which query is being executes
//
// Returns: []string column containing formatted selected columns for spanner query
//
//	and a map of aliases to AsKeywordMeta structs
func processStrings(t *Translator, selectedColumns []tableConfig.SelectedColumns, tableName string) (map[string]tableConfig.AsKeywordMeta, []string, error) {
	lenInputString := len(selectedColumns)
	var columns = make([]string, 0)
	structs := make([]tableConfig.AsKeywordMeta, 0, lenInputString)
	for _, columnMetadata := range selectedColumns {
		var dataType datatype.DataType
		var cqlType string
		var err error
		if columnMetadata.IsFunc {
			cqlType, err = inferDataType(columnMetadata.FuncName)
			if err != nil {
				return nil, nil, err
			}
			dataType, err = utilities.GetCassandraColumnType(cqlType)
			if err != nil {
				return nil, nil, err
			}
			if columnMetadata.Name == STAR {
				columns = append(columns, fmt.Sprintf("%s(%s) as %s", columnMetadata.FuncName, columnMetadata.Name, columnMetadata.Alias))
			} else {
				if tableName == columnMetadata.Name {
					columns = append(columns, fmt.Sprintf("%s(`%s`.`%s`) as %s", columnMetadata.FuncName, columnMetadata.Name, columnMetadata.Name, columnMetadata.Alias))
				} else {
					columns = append(columns, fmt.Sprintf("%s(`%s`) as %s", columnMetadata.FuncName, columnMetadata.Name, columnMetadata.Alias))
				}
			}
		} else {
			if columnMetadata.Name == spannerTSColumn {
				dataType = datatype.Bigint
				cqlType = utilities.BigintType
			} else {
				metaStr, err := t.TableConfig.GetColumnType(tableName, columnMetadata.Name)
				if err != nil {
					return nil, nil, err
				}
				cqlType = metaStr.CQLType
				dataType, err = utilities.GetCassandraColumnType(metaStr.CQLType)
				if err != nil {
					return nil, nil, err
				}

			}

			if columnMetadata.IsAs {
				if tableName == columnMetadata.Name {
					columnMetadata.FormattedColumn = fmt.Sprintf("`%s`.`%s` as %s", columnMetadata.Name, columnMetadata.Name, columnMetadata.Alias)
				} else {
					columnMetadata.FormattedColumn = fmt.Sprintf("`%s` as %s", columnMetadata.Name, columnMetadata.Alias)
				}
				columns = append(columns, columnMetadata.FormattedColumn)
			} else {
				if tableName == columnMetadata.Name {
					columnMetadata.FormattedColumn = "`" + columnMetadata.Name + "`.`" + columnMetadata.Name + "`"
				} else {
					columnMetadata.FormattedColumn = "`" + columnMetadata.Name + "`"
				}
				columns = append(columns, columnMetadata.FormattedColumn)
			}
		}

		structs = append(structs, tableConfig.AsKeywordMeta{
			IsFunc:   columnMetadata.IsFunc,
			Name:     columnMetadata.Name,
			Alias:    columnMetadata.Alias,
			DataType: dataType,
			CQLType:  cqlType,
		})
	}
	var aliasMap = make(map[string]tableConfig.AsKeywordMeta)
	for _, meta := range structs {
		if meta.Alias != "" {
			aliasMap[meta.Alias] = meta
		}
	}
	return aliasMap, columns, nil
}

// inferDataType() returns the data type based on the name of a function.
//
// Parameters:
//   - methodName: Name of aggregate function
//
// Returns: Returns datatype of aggregate function.
func inferDataType(methodName string) (string, error) {
	switch methodName {
	case "count":
		return "bigint", nil
	case "round":
		return "float", nil
	default:
		return "", fmt.Errorf("Unknown function '%s'", methodName)
	}
}

// Returns Spanner Select query using Parsed information.
//
// Parameters:
//   - data: SelectQueryMap struct with all select query info from CQL query
func getSpannerSelectQuery(t *Translator, data *SelectQueryMap) (string, error) {
	column := ""
	var columns []string
	var err error
	var aliasMap map[string]tableConfig.AsKeywordMeta

	if data.ColumnMeta.Star {
		column = STAR
		// TODO: Instead of exclude keyword, flattern list of all columns from TableConfigurations.
		if t.UseRowTimestamp && t.UseRowTTL {
			column += excludeInternalColumns
		} else if t.UseRowTimestamp {
			column += includeTSColumn
		} else if t.UseRowTTL {
			column += includeTTLColumn
		}
	} else {
		aliasMap, columns, err = processStrings(t, data.ColumnMeta.Column, data.Table)
		if err != nil {
			return "nil", err
		}
		data.AliasMap = aliasMap
		column = strings.Join(columns, ",")
	}

	if column != "" && data.Table != "" {
		spannerQuery := fmt.Sprintf("SELECT %s FROM %s", column, data.Table)
		whereCondition := buildWhereClause(data.Clauses)
		if whereCondition != "" {
			spannerQuery += whereCondition
		}

		if data.OrderBy.isOrderBy {
			spannerQuery = spannerQuery + " ORDER BY " + data.OrderBy.Column + " " + data.OrderBy.Operation
		}

		if data.Limit.isLimit {
			val := data.Limit.Count
			if val == questionMark {
				val = "@" + limitPlaceholder
			}
			spannerQuery = spannerQuery + " " + "LIMIT" + " " + val
		}

		spannerQuery += ";"
		return spannerQuery, nil
	}
	return "", errors.New("could not prepare the select query due to incomplete information")

}

// Translates Cassandra select statement into a compatible Cloud Spanner select query.
//
// Parameters:
//   - originalQuery: CQL Select statement
//
// Returns: SelectQueryMap struct and error if any
func (t *Translator) ToSpannerSelect(keyspace string, originalQuery string) (*SelectQueryMap, error) {
	lowerQuery := strings.ToLower(originalQuery)
	//Create copy of cassandra query where literals are substituted with a suffix
	query := renameLiterals(originalQuery)
	// special handling for writetime
	query = replaceWriteTimePatterns(query)

	p, err := NewCqlParser(query, t.Debug)
	if err != nil {
		return nil, err
	}
	selectObj := p.Select_()
	if selectObj == nil {
		return nil, errors.New("ToSpannerSelect: Could not parse select object")
	}

	kwSelectObj := selectObj.KwSelect()
	if kwSelectObj == nil {
		return nil, errors.New("ToSpannerSelect: Could not parse select object")
	}

	queryType := kwSelectObj.GetText()
	columns, err := parseColumnsFromSelect(selectObj.SelectElements())
	if err != nil {
		return nil, err
	}

	tableSpec, err := parseTableFromSelect(keyspace, selectObj.FromSpec())
	if err != nil {
		return nil, err
	} else if tableSpec.TableName == "" || tableSpec.KeyspaceName == "" {
		return nil, errors.New("ToSpannerSelect: No table or keyspace name found in the query")
	}
	tableSpec.TableName = utilities.FlattenTableName(t.KeyspaceFlatter, tableSpec.KeyspaceName, tableSpec.TableName)

	var clauseResponse ClauseResponse

	if hasWhere(lowerQuery) {
		resp, err := parseWhereByClause(selectObj.WhereSpec(), tableSpec.TableName, t.TableConfig)
		if err != nil {
			return nil, err
		}
		clauseResponse = *resp
	}

	var orderBy OrderBy
	if hasOrderBy(lowerQuery) {
		orderBy = parseOrderByFromSelect(selectObj.OrderSpec())
	} else {
		orderBy.isOrderBy = false
	}

	var limit Limit
	if hasLimit(lowerQuery) {
		limit = parseLimitFromSelect(selectObj.LimitSpec())
		if limit.Count == questionMark {
			clauseResponse.ParamKeys = append(clauseResponse.ParamKeys, limitPlaceholder)
		}
	} else {
		limit.isLimit = false
	}

	selectQueryData := &SelectQueryMap{
		CassandraQuery: query,
		SpannerQuery:   "",
		QueryType:      queryType,
		Table:          tableSpec.TableName,
		Keyspace:       tableSpec.KeyspaceName,
		ColumnMeta:     columns,
		Clauses:        clauseResponse.Clauses,
		Limit:          limit,
		OrderBy:        orderBy,
		Params:         clauseResponse.Params,
		ParamKeys:      clauseResponse.ParamKeys,
	}

	translatedResult, err := getSpannerSelectQuery(t, selectQueryData)
	if err != nil {
		return nil, err
	}

	selectQueryData.SpannerQuery = translatedResult
	return selectQueryData, nil
}
