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
	"strconv"
	"strings"

	"github.com/ollionorg/cassandra-to-spanner-proxy/utilities"
)

var (
	ErrEmptyTable           = errors.New("could not find table name to create spanner query")
	ErrParsingDelObj        = errors.New("error while parsing delete object")
	ErrParsingTs            = errors.New("timestamp could not be parsed")
	ErrTsNoValue            = errors.New("no value found for Timestamp")
	ErrEmptyTableOrKeyspace = errors.New("ToSpannerDelete: No table or keyspace name found in the query")
)

// createSpannerDeleteQuery generates the Spanner delete query using Parsed information.
//
// It takes the table name and an array of clauses as input and returns the generated query string.
func createSpannerDeleteQuery(table string, clauses []Clause) string {
	return "DELETE FROM " + table + buildWhereClause(clauses) + ";"
}

// ToSpannerDelete frames Spanner supported query for Delete Query
//
// Parameters:
//   - query: CQL Delete query
//
// Returns: DeleteQueryMap struct and error if any
func (t *Translator) ToSpannerDelete(queryStr string) (*DeleteQueryMap, error) {
	lowerQuery := strings.ToLower(queryStr)
	query := renameLiterals(queryStr)
	p, err := NewCqlParser(query, t.Debug)
	if err != nil {
		return nil, err
	}

	deleteObj := p.Delete_()
	if deleteObj == nil {
		return nil, ErrParsingDelObj
	}
	kwDeleteObj := deleteObj.KwDelete()
	if kwDeleteObj == nil {
		return nil, ErrParsingDelObj
	}
	queryType := kwDeleteObj.GetText()
	tableSpec, err := parseTableFromSelect(deleteObj.FromSpec())
	if err != nil {
		return nil, err
	} else if tableSpec.TableName == "" || tableSpec.KeyspaceName == "" {
		return nil, ErrEmptyTableOrKeyspace
	}
	tableSpec.TableName = utilities.FlattenTableName(t.KeyspaceFlatter, tableSpec.KeyspaceName, tableSpec.TableName)

	tsValue := ""
	if hasUsingTimestamp(lowerQuery) {
		spec := deleteObj.UsingTimestampSpec()
		if spec == nil {
			return nil, ErrParsingTs
		}
		timestampSpec := spec.Timestamp()
		if timestampSpec == nil {
			return nil, ErrParsingTs
		}

		if spec.KwUsing() != nil {
			spec.KwUsing()
		}
		valLiteral := timestampSpec.DecimalLiteral()
		if valLiteral == nil {
			return nil, ErrTsNoValue
		}

		tsValue = valLiteral.GetText()
	}

	var clauseResponse ClauseResponse

	if hasWhere(lowerQuery) {
		resp, err := parseWhereByClause(deleteObj.WhereSpec(), tableSpec.TableName, t.TableConfig)
		if err != nil {
			return nil, err
		}
		clauseResponse = *resp
	}

	if tsValue != "" {
		newKeys := []string{tsValuePlaceholder}
		clauseResponse.ParamKeys = append(newKeys, clauseResponse.ParamKeys...)
		if tsValue != questionMark {
			ts64, err := strconv.ParseInt(tsValue, 10, 64)
			if err != nil {
				return nil, err
			}

			formattedTS, err := utilities.FormatTimestamp(ts64)
			if err != nil {
				return nil, err
			}

			clauseResponse.Params[tsValuePlaceholder] = formattedTS
		}

		clauseResponse.Clauses = append([]Clause{
			{
				Column:       spannerTSColumn,
				Operator:     "<=",
				Value:        "@" + tsValuePlaceholder,
				IsPrimaryKey: false,
			},
		}, clauseResponse.Clauses...)
	}

	primaryKeys, err := getPrimaryKeys(t.TableConfig, tableSpec.TableName)
	if err != nil {
		return nil, err
	}

	deleteQueryData := &DeleteQueryMap{
		CassandraQuery: query,
		SpannerQuery:   createSpannerDeleteQuery(tableSpec.TableName, clauseResponse.Clauses),
		QueryType:      queryType,
		Table:          tableSpec.TableName,
		Keyspace:       tableSpec.KeyspaceName,
		Clauses:        clauseResponse.Clauses,
		Params:         clauseResponse.Params,
		ParamKeys:      clauseResponse.ParamKeys,
		PrimaryKeys:    primaryKeys,
	}
	return deleteQueryData, nil
}
