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
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/antlr4-go/antlr/v4"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	cql "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/translator/cqlparser"
)

const (
	literalPlaceholder  = "_a1b2c3"
	ttlValuePlaceholder = "ttlValue"
	tsValuePlaceholder  = "tsValue"
	spannerTTLColumn    = "spanner_ttl_ts"
	spannerTSColumn     = "last_commit_ts"
	limitPlaceholder    = "limitValue"
	writetime           = "writetime"
	commitTsFn          = "PENDING_COMMIT_TIMESTAMP()"
	missingUndefined    = "<missing undefined>"
	questionMark        = "?"
	STAR                = "*"
	mapKeyPlaceholder   = "map_key"
)

var (
	whereRegex          = regexp.MustCompile(`(?i)\bwhere\b`)
	orderByRegex        = regexp.MustCompile(`(?i)\border\s+by\b`)
	limitRegex          = regexp.MustCompile(`(?i)\blimit\b`)
	usingTimestampRegex = regexp.MustCompile(`using.*timestamp`)
	ifNotExistsRegex    = regexp.MustCompile(`if.*not.*exists`)
	usingTtlRegex       = regexp.MustCompile(`using.*ttl`)
	ttlFirstRegex       = regexp.MustCompile(`(?i)\busing\s+ttl.*\s+and\s+timestamp\b`)
	// asOperatorRegex     = regexp.MustCompile(`(?i)\bAS\b`)

	// This regular expression matches 'writetime(column_name)' and captures 'column_name' for reuse.
	// It also optionally matches ' as alias' and captures 'alias' for reuse.
	writeTimeRegex = regexp.MustCompile(`(?i)writetime\((\w+)\)(\s+as\s+(\w+))?`)

	//regular expression that matches any of the specified literals
	combinedRegex = createCombinedRegex()
)

var layouts = []string{
	time.RFC3339,          // ISO 8601 format
	"2006-01-02 15:04:05", // Common date-time format
	"2006/01/02 15:04:05", // Common date-time format with slashes
}

// Function to convert map data into String
func convertMapString(dataStr string) (map[string]interface{}, error) {
	// Split the string into key-value pairs
	pairs := strings.Split(strings.Trim(dataStr, "{}"), ", ")

	// Initialize an empty map for storing data
	data := map[string]interface{}{}

	// // Extract key-value pairs and add them to the map
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			parts = strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				// Handle the case where the split doesn't result in 2 parts
				return nil, fmt.Errorf("error while formatting %v", parts)
			}
		}

		v := strings.TrimSpace(strings.ToLower(parts[1]))
		k := strings.ReplaceAll(parts[0], `"`, ``)
		if v == "true" {
			data[k] = true
		} else {
			data[k] = false
		}

	}

	return data, nil
}

// function to convert json tring to cassandra map type
func convertToMap(dataStr string, cqlType string) (map[string]interface{}, error) {
	// Split the string into key-value pairs
	dataStr = strings.ReplaceAll(dataStr, ": \"", ":\"")
	dataStr = strings.ReplaceAll(dataStr, "\"", "")
	pairs := strings.Split(strings.Trim(dataStr, "{}"), ", ")
	// Initialize an empty map for storing data
	switch cqlType {
	case "map<text, timestamp>":
		return mapOfStringToTimestamp(pairs)
	case "map<text, text>":
		return mapOfStringToString(pairs)
	default:
		return nil, fmt.Errorf("unhandled map type")
	}
}

func mapOfStringToString(pairs []string) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			parts = strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				// Handle the case where the split doesn't result in 2 parts
				return nil, fmt.Errorf("error while formatting %v", parts)
			}

		}
		data[parts[0]] = parts[1]
	}
	return data, nil
}

func mapOfStringToTimestamp(pairs []string) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			parts = strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				// Handle the case where the split doesn't result in 2 parts
				return nil, fmt.Errorf("error while formatting %v", parts)
			}
		}
		timestamp, err := parseTimestamp(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, fmt.Errorf("error while typecasting to timestamp %v, -> %s", parts[1], err)
		}
		data[parts[0]] = timestamp
	}
	return data, nil
}

// Parse a timestamp string in various formats.
// Supported formats
// "2024-02-05T14:00:00Z",
// "2024-02-05 14:00:00",
// "2024/02/05 14:00:00",
// "1672522562",             // Unix timestamp (seconds)
// "1672522562000",          // Unix timestamp (milliseconds)
// "1672522562000000",       // Unix timestamp (microseconds)
func parseTimestamp(timestampStr string) (time.Time, error) {
	var parsedTime time.Time
	var err error

	// Try to parse the timestamp using each layout
	for _, layout := range layouts {
		parsedTime, err = time.Parse(layout, timestampStr)
		if err == nil {
			return parsedTime, nil
		}
	}

	// Try to parse as Unix timestamp (in seconds, milliseconds, or microseconds)
	if unixTime, err := strconv.ParseInt(timestampStr, 10, 64); err == nil {
		switch len(timestampStr) {
		case 10: // Seconds
			return time.Unix(unixTime, 0).UTC(), nil
		case 13: // Milliseconds
			return time.Unix(0, unixTime*int64(time.Millisecond)).UTC(), nil
		case 16: // Microseconds
			return time.Unix(0, unixTime*int64(time.Microsecond)).UTC(), nil
		}
	}

	// If all formats fail, return the last error
	return time.Time{}, err
}

// Function to check if where clause exist in the query.
func hasWhere(query string) bool {
	return whereRegex.MatchString(query)
}

// Function to check if order by exist in the query.
func hasOrderBy(query string) bool {
	return orderByRegex.MatchString(query)
}

// Function to check if limit exist in the query.
func hasLimit(query string) bool {
	return limitRegex.MatchString(query)
}

// Function to check if using timestamp exist in the query.
func hasUsingTimestamp(query string) bool {
	return usingTimestampRegex.MatchString(query)
}

// Function to check if IF NOT EXISTS exist in the query.
func hasIfNotExists(query string) bool {
	return ifNotExistsRegex.MatchString(query)
}

// Function to check if using ttl exist in the query.
func hasUsingTtl(query string) bool {
	return usingTtlRegex.MatchString(query)
}

// Function to check if TTL is used before timestamp
func isTTLFirst(query string) bool {
	return ttlFirstRegex.MatchString(query)
}

// Remove all duplicate values from the list and keep only unique values.
func getUniqueSet(elements []string) []string {
	encountered := map[string]bool{}
	result := []string{}
	for _, v := range elements {
		v = strings.TrimSpace(v)
		if !encountered[v] {
			encountered[v] = true
			result = append(result, v)
		}
	}
	return result
}

func valueIsNull(value string) bool {
	return strings.ToLower(value) == "null"
}

// Function to format the cassandra values as per its spanner type.
func formatValues(value string, spannerType string, cqlType string) (interface{}, error) {
	if valueIsNull(value) {
		return nil, nil
	}
	switch spannerType {
	case "string":
		return value, nil
	case "timestamp":
		return value, nil
	case "bytes":
		val := []byte(string(value))
		return val, nil
	case "int64":
		i, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		return i, nil
	case "bool":
		val := (value == "true")
		return val, nil
	case "float64":
		i, err := strconv.ParseFloat(value, 64) // 64 specifies the precision
		if err != nil {
			return nil, err
		}
		return i, nil
	case "array[string]":
		inputText := strings.Trim(value, "[]")
		inputText = strings.Trim(inputText, "{}")
		inputText = strings.ReplaceAll(inputText, `"`, "")
		splitValues := strings.Split(inputText, ",")
		if cqlType == "set<text>" {
			splitValues = getUniqueSet(splitValues)
		}

		var result []string
		for _, val := range splitValues {
			result = append(result, strings.TrimSpace(val))
		}
		return result, nil
	case "array[string, bool]":
		convertedMap, err := convertMapString(value)
		if err != nil {
			return nil, err
		}
		val := spanner.NullJSON{Value: convertedMap, Valid: true}
		return val, nil
	case "array[string, timestamp]", "array[string, string]":
		convertedMap, err := convertToMap(value, cqlType)
		if err != nil {
			return nil, err
		}
		val := spanner.NullJSON{Value: convertedMap, Valid: true}
		return val, nil
	default:
		err := errors.New("no correct Datatype found for column")
		return nil, err
	}
}

// createCombinedRegex constructs a regular expression that matches any of the specified keywords as whole words.
// The keywords are "time", "key", "type", and "json".
// The resulting regex will match these keywords only if they appear as complete words in the input string.
//
// Returns:
//
//	*regexp.Regexp: A compiled regular expression object.
func createCombinedRegex() *regexp.Regexp {
	keywords := []string{"time", "key", "type", "json", "date", "trigger"}
	combinedPattern := fmt.Sprintf("\\b(%s)\\b", regexp.QuoteMeta(keywords[0]))

	for _, keyword := range keywords[1:] {
		combinedPattern += fmt.Sprintf("|\\b%s\\b", regexp.QuoteMeta(keyword))
	}
	return regexp.MustCompile(combinedPattern)
}

// Function to find and replace the cql literals and add a temporary placeholder.
func renameLiterals(query string) string {
	return combinedRegex.ReplaceAllStringFunc(query, func(match string) string {
		return match + literalPlaceholder
	})
}

// This function searches and replaces patterns of 'writetime(column_name)' and
// 'writetime(column_name) as alias' in a given SQL query.
func replaceWriteTimePatterns(query string) string {
	// Replace matched patterns with 'last_commit_ts as writetime_column_name' or 'last_commit_ts as alias'
	// depending on whether an alias is present.
	replacedQuery := writeTimeRegex.ReplaceAllStringFunc(query, func(match string) string {
		submatches := writeTimeRegex.FindSubmatch([]byte(match))
		columnName := string(submatches[1])
		// Check if an alias is captured.
		if len(submatches) > 3 && len(submatches[3]) > 0 {
			alias := string(submatches[3])
			// If an alias is present, use it directly.
			return fmt.Sprintf("last_commit_ts as %s", alias)
		}
		// If no alias is present, construct one using the column name.
		return fmt.Sprintf("last_commit_ts as writetime_%s", columnName)
	})
	return replacedQuery
}

// Function to get primary keys from table config.
func getPrimaryKeys(tableConfig *tableConfig.TableConfig, tableName string) ([]string, error) {
	var primaryKeys []string
	pks, err := tableConfig.GetPkByTableName(tableName)
	if err != nil {
		return nil, err
	}

	for _, pk := range pks {
		primaryKeys = append(primaryKeys, pk.Name)
	}

	return primaryKeys, nil
}

// buildWhereClause takes a slice of Clause structs and returns a string representing the WHERE clause of a Spanner SQL query.
// It iterates over the clauses and constructs the WHERE clause by combining the column name, operator, and value of each clause.
// If the operator is "IN", the value is wrapped with the UNNEST function.
// The constructed WHERE clause is returned as a string.
func buildWhereClause(clauses []Clause) string {
	whereClause := ""
	for _, val := range clauses {
		column := "`" + val.Column + "`"
		value := val.Value

		if val.Operator == "IN" {
			value = fmt.Sprintf("UNNEST(%s)", val.Value)
		}
		if whereClause != "" {
			whereClause += " AND "
		}
		whereClause += fmt.Sprintf("%s %s %s", column, val.Operator, value)
	}

	if whereClause != "" {
		whereClause = " WHERE " + whereClause
	}
	return whereClause
}

// Funtion to create lexer and parser object for the cql query
func NewCqlParser(cqlQuery string, isDebug bool) (*cql.CqlParser, error) {
	if cqlQuery == "" {
		return nil, fmt.Errorf("invalid input string")
	}

	lexer := cql.NewCqlLexer(antlr.NewInputStream(cqlQuery))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)
	if p == nil {
		return nil, fmt.Errorf("error while creating parser object")
	}

	if !isDebug {
		p.RemoveErrorListeners()
	}
	return p, nil
}

// parseWhereByClause parse Clauses from the Query
//
// Parameters:
//   - input: The Where Spec context from the antlr Parser.
//   - tableName - Table Name
//   - tableConfig - JSON Config which maintains column and its datatypes info.
//
// Returns: ClauseResponse and an error if any.
func parseWhereByClause(input cql.IWhereSpecContext, tableName string, tableConfig *tableConfig.TableConfig) (*ClauseResponse, error) {
	if input == nil {
		return nil, errors.New("no input parameters found for clauses")
	}

	elements := input.RelationElements().AllRelationElement()
	if elements == nil {
		return nil, errors.New("no input parameters found for clauses")
	}

	if len(elements) == 0 {
		return &ClauseResponse{}, nil
	}

	var clauses []Clause
	var response ClauseResponse
	var paramKeys []string

	params := make(map[string]interface{})

	for i, val := range elements {
		if val == nil {
			return nil, errors.New("could not parse column object")
		}
		s := strconv.Itoa(i + 1)
		placeholder := "value" + s
		operator := ""
		paramKeys = append(paramKeys, placeholder)
		colObj := val.OBJECT_NAME(0)
		if colObj == nil {
			return nil, errors.New("could not parse column object")
		}
		isInOperator := false

		if val.OPERATOR_EQ() != nil {
			operator = val.OPERATOR_EQ().GetText()
		} else if val.OPERATOR_GT() != nil {
			operator = val.OPERATOR_GT().GetSymbol().GetText()
		} else if val.OPERATOR_LT() != nil {
			operator = val.OPERATOR_LT().GetSymbol().GetText()
		} else if val.OPERATOR_GTE() != nil {
			operator = val.OPERATOR_GTE().GetSymbol().GetText()
		} else if val.OPERATOR_LTE() != nil {
			operator = val.OPERATOR_LTE().GetSymbol().GetText()
		} else if val.KwIn() != nil {
			operator = "IN"
			isInOperator = true
		} else {
			return nil, errors.New("no supported operator found")
		}

		colName := strings.ToLower(colObj.GetText())
		if colName == "" {
			return nil, errors.New("could not parse column name")
		}
		colName = strings.ReplaceAll(colName, literalPlaceholder, "")
		columnType, err := tableConfig.GetColumnType(tableName, colName)
		if err != nil {
			return nil, err
		}
		if columnType != nil && columnType.SpannerType != "" && columnType.CQLType != "" {
			if !isInOperator {
				valConst := val.Constant()
				if valConst == nil {
					return nil, errors.New("could not parse value from query for one of the clauses")
				}
				value := val.Constant().GetText()
				if value == "" {
					return nil, errors.New("could not parse value from query for one of the clauses")
				}
				value = strings.ReplaceAll(value, "'", "")

				if value != questionMark {
					val, err := formatValues(value, columnType.SpannerType, columnType.CQLType)
					if err != nil {
						return nil, err
					}
					params[placeholder] = val
				} else {
					params[placeholder] = "?"
				}

			} else {
				lower := strings.ToLower(val.GetText())
				if !strings.Contains(lower, "in?") {

					valueFn := val.FunctionArgs()
					if valueFn == nil {
						return nil, errors.New("could not parse Function arguments")
					}
					value := valueFn.AllConstant()
					if value == nil {
						return nil, errors.New("could not parse all values inside IN operator")
					}

					switch columnType.SpannerType {
					case "string", "timestamp":
						var allValues []string
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							allValues = append(allValues, valueTxt)
						}
						params[placeholder] = allValues
					case "bytes":
						var allValues [][]byte
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							val := []byte(string(valueTxt))
							allValues = append(allValues, val)
						}
						params[placeholder] = allValues
					case "int64":
						var allValues []int
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							i, err := strconv.Atoi(valueTxt)
							if err != nil {
								return nil, err
							}
							allValues = append(allValues, i)
						}
						params[placeholder] = allValues
					case "bool":
						var allValues []bool
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							val := (valueTxt == "true")
							allValues = append(allValues, val)
						}
						params[placeholder] = allValues
					case "array[string]":
						var allValues [][]string
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							inputText := strings.Trim(valueTxt, "[]")
							inputText = strings.Trim(inputText, "{}")
							inputText = strings.ReplaceAll(inputText, `"`, "")
							splitValues := strings.Split(inputText, ",")
							if columnType.CQLType == "set<text>" {
								splitValues = getUniqueSet(splitValues)
							}

							var result []string
							for _, val := range splitValues {
								result = append(result, strings.TrimSpace(val))
							}
							allValues = append(allValues, result)
						}
						params[placeholder] = allValues
					case "array[string, bool]":
						var allValues []spanner.NullJSON
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							convertedMap, err := convertMapString(valueTxt)
							if err != nil {
								return nil, err
							}
							val := spanner.NullJSON{Value: convertedMap, Valid: true}
							allValues = append(allValues, val)
						}
						params[placeholder] = allValues
					case "array[string, timestamp]", "array[string, string]":
						var allValues []spanner.NullJSON
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							convertedMap, err := convertToMap(valueTxt, columnType.CQLType)
							if err != nil {
								return nil, err
							}
							val := spanner.NullJSON{Value: convertedMap, Valid: true}
							allValues = append(allValues, val)
						}
						params[placeholder] = allValues
					case "float64":
						var allValues []float64
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							valueTxt = strings.ReplaceAll(valueTxt, "'", "")
							i, err := strconv.ParseFloat(valueTxt, 64) // 64 specifies the precision
							if err != nil {
								return nil, err
							}
							allValues = append(allValues, i)
						}
						params[placeholder] = allValues
					default:
						err := errors.New("no correct Datatype found for column")
						return nil, err
					}
				}
			}
			clause := &Clause{
				Column:       colName,
				Operator:     operator,
				Value:        "@" + placeholder,
				IsPrimaryKey: columnType.IsPrimaryKey,
			}
			clauses = append(clauses, *clause)
		}
	}
	response.Clauses = clauses
	response.Params = params
	response.ParamKeys = paramKeys
	return &response, nil
}

// Helper function to convert CQL type to Spanner DDL type
func spannerDDLType(column Column) (string, error) {
	switch column.SpannerType {
	case "string", "text", "varchar":
		return "STRING(MAX)", nil
	case "timestamp":
		return "TIMESTAMP", nil
	case "int64", "bigint":
		return "INT64", nil
	case "bytes":
		return "BYTES(MAX)", nil
	case "json":
		return "JSON", nil
	case "float64", "double":
		return "FLOAT64", nil
	case "bool":
		return "BOOL", nil
	case "numeric":
		return "NUMERIC", nil
	case "date":
		return "DATE", nil
	case "frozen<list<text>>", "frozen<set<text>>", "list<text>", "set<text>", "array<string>", "array[string]":
		return "ARRAY<STRING(MAX)>", nil // Handle list/set as ARRAY in Spanner
	case "frozen<list<boolean>", "frozen<set<boolean>>", "list<boolean>", "set<boolean>":
		return "ARRAY<BOOL>", nil // Handle list/set of booleans
	case "frozen<list<int>>", "frozen<set<int>>", "list<int>", "set<int>":
		return "ARRAY<INT64>", nil // Handle list/set of integers
	case "frozen<list<timestamp>>", "frozen<set<timestamp>>", "list<timestamp>", "set<timestamp>":
		return "ARRAY<TIMESTAMP>", nil // Handle list/set of timestamps
	case "map<text, boolean>", "map<text, timestamp>":
		// Store map as JSON in Spanner
		return "JSON", nil
	case "array[string, bool]", "array[string, timestamp]", "array[string, string]":
		return "JSON", nil
	default:
		return "", fmt.Errorf("unsupported Cassandra type: %s", column.SpannerType)
	}
}

func RemoveQuotesFromKeyspace(id string) string {
	l := len(id)
	if l > 0 && id[0] == '"' {
		return id[1 : l-1]
	} else {
		return id
	}
}
