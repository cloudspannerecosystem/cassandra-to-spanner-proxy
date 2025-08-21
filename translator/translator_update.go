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
	"fmt"
	"strconv"
	"strings"
	"time"

	"errors"

	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	cql "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/translator/cqlparser"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/utilities"
)

func parseAssignments(assignments []cql.IAssignmentElementContext, tableName string, tableConfig *tableConfig.TableConfig) (*UpdateSetResponse, error) {
	if len(assignments) == 0 {
		return nil, errors.New("invalid input")
	}
	var setResp []UpdateSetValue
	mapUpdateColumn := ""
	var paramKeys []string
	params := make(map[string]interface{})

	for i, val := range assignments {
		isMapUpdateByKey := false
		mapUpdateKey := ""
		assignStr := val.GetText()
		s := strconv.Itoa(i + 1)
		placeholder := "set" + s
		colObj := val.OBJECT_NAME(0)
		if colObj == nil {
			return nil, errors.New("error parsing column for assignments")
		}
		columnName := colObj.GetText()

		if columnName == "" {
			return nil, errors.New("no columnName found for assignments")
		}

		columnName = strings.ReplaceAll(columnName, literalPlaceholder, "")
		columnType, err := tableConfig.GetColumnType(tableName, columnName)

		var value string
		if strings.Contains(assignStr, "=?") {
			value = questionMark
		} else {
			valConst := val.Constant()
			if valConst == nil {
				return nil, errors.New("error parsing value for assignments")
			}
			value = valConst.GetText()
			value = strings.ReplaceAll(value, "'", "")

		}

		if strings.Contains(columnType.CQLType, "map") {

			if val.SyntaxBracketLs() != nil && val.SyntaxBracketRs() != nil {
				if mapUpdateColumn != "" {
					return nil, errors.New("multiple map update via key is not supported in a single query")
				}

				if value == "?" || value == "" {
					return nil, errors.New("value should be given in raw form for map update by key")
				}
				isMapUpdateByKey = true
				mapUpdateColumn = columnName
				if val.DecimalLiteral() != nil {
					mapUpdateKey = val.DecimalLiteral().GetText()
				}
				placeholder = mapKeyPlaceholder
			}
		}

		if value != questionMark && !isMapUpdateByKey {
			if err != nil {
				return nil, err
			}

			val, err := formatValues(value, columnType.SpannerType, columnType.CQLType)
			if err != nil {
				return nil, err
			}

			params[placeholder] = val
		}

		paramKeys = append(paramKeys, placeholder)

		setResp = append(setResp, UpdateSetValue{
			Column:           columnName,
			Value:            "@" + placeholder,
			IsMapUpdateByKey: isMapUpdateByKey,
			MapUpdateKey:     mapUpdateKey,
			RawValue:         value,
		})

	}

	return &UpdateSetResponse{
		UpdateSetValues: setResp,
		ParamKeys:       paramKeys,
		Params:          params,
		MapUpdateColumn: mapUpdateColumn,
	}, nil
}

func buildSetValues(updateSetValues []UpdateSetValue) string {
	setValues := ""
	for _, val := range updateSetValues {
		column := "`" + val.Column + "`"
		value := val.Value

		if setValues != "" {
			setValues += " , "
		}
		setValues += fmt.Sprintf("%s = %s", column, value)
	}
	if setValues != "" {
		setValues = " SET " + setValues
	}
	return setValues
}

// createSpannerUpdateQuery generates a Spanner update query based on the provided table name, update set values, and clauses.
// It returns the generated update query as a string.
func createSpannerUpdateQuery(table string, updateSetValues []UpdateSetValue, clauses []Clause) string {
	return "UPDATE " + table + buildSetValues(updateSetValues) + buildWhereClause(clauses) + ";"
}

func createSpannerSelectQueryForMapUpdate(table string, columns string, clauses []Clause) string {
	if columns != "" {
		return "SELECT " + columns + " FROM " + table + buildWhereClause(clauses) + ";"
	}
	return ""
}

// ToSpannerUpdate frames Spanner supported query for Update Query
//
// Parameters:
//   - query: CQL Update query
//
// Returns: UpdateQueryMap struct and error if any
func (t *Translator) ToSpannerUpdate(keyspace string, queryStr string, isSimpleQuery bool) (*UpdateQueryMap, error) {
	lowerQuery := strings.ToLower(queryStr)
	query := renameLiterals(queryStr)
	p, err := NewCqlParser(query, t.Debug)
	if err != nil {
		return nil, err
	}

	updateObj := p.Update()
	if updateObj == nil {
		return nil, errors.New("error parsing the update object")
	}

	kwUpdateObj := updateObj.KwUpdate()
	if kwUpdateObj == nil {
		return nil, errors.New("error parsing the update object")
	}

	queryType := kwUpdateObj.GetText()
	updateObj.DOT()

	table := updateObj.Table()
	if table == nil {
		return nil, errors.New("invalid input parameter found for table")
	}

	var keyspaceName string
	if updateObj.Keyspace() != nil {
		keyspaceObj := updateObj.Keyspace().OBJECT_NAME()
		if keyspaceObj == nil {
			return nil, errors.New("invalid input parameter found for keyspace")
		}
		keyspaceName = keyspaceObj.GetText()
	} else if len(keyspace) != 0 {
		keyspaceName = RemoveQuotesFromKeyspace(keyspace)
	} else {
		return nil, errors.New("invalid input parameter found for keyspace")
	}

	tableobj := table.OBJECT_NAME()
	if tableobj == nil {
		return nil, errors.New("invalid input parameter found for table")
	}
	tableName := tableobj.GetText()

	tableName = utilities.FlattenTableName(t.KeyspaceFlatter, keyspaceName, tableName)

	var ttlValue string
	var tsValue string
	var formattedTS *time.Time

	ttlTimestampObj := updateObj.UsingTtlTimestamp()

	hasUsingTtl := hasUsingTtl(lowerQuery)
	if hasUsingTtl && !t.UseRowTTL {
		return nil, errors.New("'USING TTL' is not enabled. Set 'useRowTTL' to true in config.yaml")
	}

	if ttlTimestampObj != nil && hasUsingTtl {

		ttlValue, err = getTTLValue(ttlTimestampObj)

		if err != nil {
			return nil, err
		}

		if ttlValue != questionMark {
			castedToInt64, err := strconv.ParseInt(ttlValue, 10, 64)
			if err != nil {
				return nil, err
			}

			ttlValue = utilities.AddSecondsToCurrentTimestamp(castedToInt64)
		}

	}

	hasUsingTimestamp := hasUsingTimestamp(lowerQuery)
	if hasUsingTimestamp && !t.UseRowTimestamp {
		return nil, errors.New("'USING TIMESTAMP' is not enabled. Set 'useRowTimestamp' to true in config.yaml")
	}

	if ttlTimestampObj != nil && hasUsingTimestamp {
		tsValue, err = getTimestampValue(updateObj.UsingTtlTimestamp())
		if err != nil {
			return nil, err
		}

		if tsValue != questionMark {
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

	}

	updateObj.KwSet()
	assignmentObj := updateObj.Assignments()
	if assignmentObj == nil {
		return nil, errors.New("error parsing the assignment object")
	}
	allAssignmentObj := assignmentObj.AllAssignmentElement()
	if allAssignmentObj == nil {
		return nil, errors.New("error parsing all the assignment object")
	}
	setValues, err := parseAssignments(allAssignmentObj, tableName, t.TableConfig)
	if err != nil {
		return nil, err
	}

	if ttlValue != "" || tsValue != "" {
		var newParamKeys []string
		var newSet []UpdateSetValue

		if ttlValue != "" && tsValue != "" {
			if isTTLFirst(lowerQuery) {
				newParamKeys = []string{ttlValuePlaceholder, tsValuePlaceholder}
				newSet = append(newSet, UpdateSetValue{
					Column: spannerTTLColumn,
					Value:  "@" + ttlValuePlaceholder,
				})
				newSet = append(newSet, UpdateSetValue{
					Column: spannerTSColumn,
					Value:  "@" + tsValuePlaceholder,
				})
			} else {
				newParamKeys = []string{tsValuePlaceholder, ttlValuePlaceholder}
				newSet = append(newSet, UpdateSetValue{
					Column: spannerTSColumn,
					Value:  "@" + tsValuePlaceholder,
				})
				newSet = append(newSet, UpdateSetValue{
					Column: spannerTTLColumn,
					Value:  "@" + ttlValuePlaceholder,
				})

			}
			if ttlValue != questionMark {
				setValues.Params[ttlValuePlaceholder] = ttlValue
			}
			if tsValue != questionMark {
				setValues.Params[tsValuePlaceholder] = formattedTS
			}
		} else if ttlValue != "" && tsValue == "" {
			newParamKeys = []string{ttlValuePlaceholder}
			newSet = append(newSet, UpdateSetValue{
				Column: spannerTTLColumn,
				Value:  "@" + ttlValuePlaceholder,
			})

			if t.UseRowTimestamp {
				newSet = append(newSet, UpdateSetValue{
					Column: spannerTSColumn,
					Value:  commitTsFn,
				})
			}

			if ttlValue != questionMark {
				setValues.Params[ttlValuePlaceholder] = ttlValue
			}
		} else {
			newParamKeys = []string{tsValuePlaceholder}
			newSet = append(newSet, UpdateSetValue{
				Column: spannerTSColumn,
				Value:  "@" + tsValuePlaceholder,
			})
			if tsValue != questionMark {
				setValues.Params[tsValuePlaceholder] = formattedTS
			}
		}
		newSet = append(newSet, setValues.UpdateSetValues...)
		newParamKeys = append(newParamKeys, setValues.ParamKeys...)
		setValues.ParamKeys = newParamKeys
		setValues.UpdateSetValues = newSet

	} else if t.UseRowTimestamp {
		setValues.UpdateSetValues = append(setValues.UpdateSetValues, UpdateSetValue{
			Column: spannerTSColumn,
			Value:  commitTsFn,
		})
	}

	var clauseResponse ClauseResponse

	if hasWhere(lowerQuery) {
		resp, err := parseWhereByClause(updateObj.WhereSpec(), tableName, t.TableConfig, isSimpleQuery)
		if err != nil {
			return nil, err
		}

		clauseResponse = *resp

		for k, v := range clauseResponse.Params {
			setValues.Params[k] = v
		}

		if tsValue != "" {
			clauseResponse.Clauses = append(clauseResponse.Clauses, Clause{
				Column:   spannerTSColumn,
				Operator: "<=",
				Value:    "@" + tsValuePlaceholder,
			})
		}

		setValues.ParamKeys = append(setValues.ParamKeys, clauseResponse.ParamKeys...)
	} else {
		if tsValue != "" {
			clauseResponse.Clauses = append(clauseResponse.Clauses, Clause{
				Column:   spannerTSColumn,
				Operator: "<=",
				Value:    "@" + tsValuePlaceholder,
			})
		}
	}

	primaryKeys, err := getPrimaryKeys(t.TableConfig, tableName)
	if err != nil {
		return nil, err
	}

	updateQueryData := &UpdateQueryMap{
		CassandraQuery:       query,
		SpannerQuery:         createSpannerUpdateQuery(tableName, setValues.UpdateSetValues, clauseResponse.Clauses),
		QueryType:            queryType,
		Table:                tableName,
		Keyspace:             keyspaceName,
		Clauses:              clauseResponse.Clauses,
		Params:               setValues.Params,
		ParamKeys:            setValues.ParamKeys,
		UpdateSetValues:      setValues.UpdateSetValues,
		PrimaryKeys:          primaryKeys,
		SelectQueryMapUpdate: createSpannerSelectQueryForMapUpdate(tableName, setValues.MapUpdateColumn, clauseResponse.Clauses),
		MapUpdateColumn:      setValues.MapUpdateColumn,
	}

	return updateQueryData, nil
}
