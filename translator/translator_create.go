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

	"errors"

	cql "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/translator/cqlparser"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/utilities"
)

const (
	SecondsInDay int64 = 86400
)

// parseQueryForCreateTable parses column definitions and primary key details from a CREATE TABLE query.
//
// Parameters:
//   - input: Parsed CreateTableContext from antlr parser.
//
// Returns: ColumnsResponse struct containing column definitions and primary key details, or an error if parsing fails.
func parseQueryForCreateTable(input cql.ICreateTableContext) (*ColumnsResponse, error) {
	// Check if the input is nil; if so, return an error
	if input == nil {
		return nil, errors.New("parseQueryForCreateTable: input is nil")
	}

	// Retrieve the column definition list; if nil, return an error
	columns := input.ColumnDefinitionList()
	if columns == nil {
		return nil, errors.New("parseQueryForCreateTable: missing column definition list")
	}

	var primaryKeys []string       // To store primary key column names
	columnArr := make([]Column, 0) // To store column metadata

	// Iterate through each column definition
	for _, column := range columns.AllColumnDefinition() {
		columnname := strings.Replace(column.Column().GetText(), "_a1b2c3", "", 1) // Clean up column names
		if column.PrimaryKeyColumn() != nil {
			primaryKeys = append(primaryKeys, columnname) // Add primary key column to list
		}

		cqlType := column.DataType().GetText()                 // Get CQL data type
		spannerType := utilities.GetSpannerColumnType(cqlType) // Map CQL type to Spanner type

		// Create Column struct and append to columnArr
		columnArr = append(columnArr, Column{
			Name:        columnname,
			CQLType:     cqlType,
			SpannerType: spannerType,
		})
	}

	// If no primary keys were found, check if the primary key element is defined
	if len(primaryKeys) == 0 {
		// Check if PrimaryKeyElement is nil
		if columns.PrimaryKeyElement() == nil {
			return nil, errors.New("parseQueryForCreateTable: missing primary key element")
		}

		// Check if PrimaryKeyDefinition is nil
		if columns.PrimaryKeyElement().PrimaryKeyDefinition() == nil {
			return nil, errors.New("parseQueryForCreateTable: missing primary key definition")
		}

		// Split primary key definitions by comma and clean up names
		primaryColumns := strings.Split(columns.PrimaryKeyElement().PrimaryKeyDefinition().GetText(), ",")
		for _, primaryKey := range primaryColumns {
			primaryKeys = append(primaryKeys, strings.Replace(primaryKey, "_a1b2c3", "", 1))
		}
	}

	// Return response with parsed columns and primary keys
	response := &ColumnsResponse{
		Columns:       columnArr,
		PrimayColumns: primaryKeys,
	}
	return response, nil
}

// ToSpannerCreate converts a CQL CREATE TABLE query to a Spanner equivalent.
//
// Parameters:
//   - queryStr: The CQL CREATE TABLE query string.
//
// Returns: A CreatetableQueryMap containing the original CQL query and the generated Spanner query, or an error if conversion fails.
func (t *Translator) ToSpannerCreate(queryStr string, keyspaceFlatting bool, enableUsingTimestamp bool, enableUsingTTL bool) (*CreatetableQueryMap, error) {
	lowerQuery := strings.ToLower(queryStr) // Convert query string to lowercase for processing
	query := renameLiterals(lowerQuery)     // Rename literals in the query for consistency

	// Parse the query using the CQL parser
	p, err := NewCqlParser(query, t.Debug)
	if err != nil {
		return nil, err // Return error if parsing fails
	}

	// Retrieve the CREATE TABLE object from the parsed query
	createTableObj := p.CreateTable()
	if createTableObj == nil {
		return nil, errors.New("ToSpannerCreate: failed to parse create table object")
	}
	tablename := createTableObj.Table()
	if tablename == nil {
		return nil, errors.New("ToSpannerCreate: missing table name")
	}

	var keyspace string
	// Extract keyspace from the parsed object; if missing, return an error
	if ks := createTableObj.Keyspace(); ks != nil {
		keyspace = ks.GetText()
	} else {
		return nil, errors.New("ToSpannerCreate: keyspace is missing")
	}

	// Check for 'IF NOT EXISTS' clause
	hasIfNotExists := createTableObj.IfNotExist() != nil

	// Parse column definitions and primary keys from the CREATE TABLE query
	columnResponse, err := parseQueryForCreateTable(createTableObj)
	if err != nil {
		return nil, err // Return error if parsing columns fails
	}

	var ttlValue int64 // Variable to store TTL value
	// Check if there are any table options specified
	if createTableObj.WithElement() != nil && createTableObj.WithElement().TableOptions() != nil {
		withOptions := createTableObj.WithElement().TableOptions().AllTableOptionItem()
		// Iterate through options to find default TTL
		for _, option := range withOptions {
			if option.TableOptionName().GetText() == "default_time_to_live" && option.TableOptionValue() != nil {
				ttlValue, err = strconv.ParseInt(option.TableOptionValue().GetText(), 10, 64) // Convert TTL to int64
				if err != nil {
					return nil, err // Return error if parsing TTL fails
				}
			}
		}
	}

	// Prepare the create table data map with all necessary information
	createTableData := &CreatetableQueryMap{
		CassandraQuery:     queryStr,
		Table:              tablename.GetText(),
		Keyspace:           keyspace,
		KeySpaceFlattening: keyspaceFlatting,
		PrimaryKeys:        columnResponse.PrimayColumns,
		Columns:            columnResponse.Columns,
		HasIfNotExists:     hasIfNotExists,
		TTLValue:           ttlValue,
	}

	// Generate the Spanner CREATE TABLE query based on the gathered metadata
	spannerQuery, err := getSpannerCreateQuery(createTableData, enableUsingTimestamp, enableUsingTTL)
	if err != nil {
		return nil, err // Return error if generating the Spanner query fails
	}
	createTableData.SpannerQuery = spannerQuery // Assign the generated Spanner query

	return createTableData, nil // Return the completed create table data map
}

// getSpannerCreateQuery constructs the Spanner CREATE TABLE query.
//
// Parameters:
//   - metadata: CreatetableQueryMap struct containing the parsed details of the table.
//
// Returns: The Spanner CREATE TABLE query string, or an error if the generation fails.
func getSpannerCreateQuery(metadata *CreatetableQueryMap, enableUsingTimestamp bool, enableUsingTTL bool) (string, error) {
	// Start building the query using a string builder
	var sb strings.Builder
	// Handle the presence of 'IF NOT EXISTS'
	if metadata.HasIfNotExists {
		if metadata.KeySpaceFlattening {
			sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_%s (\n", metadata.Keyspace, metadata.Table))
		} else {
			sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", metadata.Table))
		}
	} else {
		if metadata.KeySpaceFlattening {
			sb.WriteString(fmt.Sprintf("CREATE TABLE %s_%s (\n", metadata.Keyspace, metadata.Table))
		} else {
			sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", metadata.Table))
		}
	}

	// Add columns to the query while ensuring backticks are used where applicable
	for i, column := range metadata.Columns {
		// Add backticks to column names except for `spanner_ttl_ts` and `last_commit_ts`
		columnName := column.Name
		if columnName != "spanner_ttl_ts" && columnName != "last_commit_ts" {
			columnName = fmt.Sprintf("`%s`", column.Name)
		}

		// Get the Spanner type for the column
		spType, err := spannerDDLType(column)
		if err != nil {
			return "", err // Return error if Spanner type conversion fails
		}

		// Append the column definition to the query string
		if i == len(metadata.Columns)-1 {
			sb.WriteString(fmt.Sprintf("\t%s %s", columnName, spType)) // Last column
		} else {
			sb.WriteString(fmt.Sprintf("\t%s %s,\n", columnName, spType)) // Other columns
		}
	}

	// Add required additional columns only once

	if enableUsingTTL {
		sb.WriteString(",\n\tspanner_ttl_ts TIMESTAMP DEFAULT (NULL)")
	}

	if enableUsingTimestamp {
		sb.WriteString(",\n\tlast_commit_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)")
	}

	if enableUsingTTL {
		// Add primary key definition to the query
		sb.WriteString(fmt.Sprintf("\n) PRIMARY KEY (%s),\n", formatPrimaryKeys(metadata.PrimaryKeys)))
		// Convert TTLValue from seconds to days for Spanner
		ttlDays := metadata.TTLValue / SecondsInDay

		// Add row deletion policy based on TTL
		sb.WriteString(fmt.Sprintf("ROW DELETION POLICY (OLDER_THAN(spanner_ttl_ts, INTERVAL %d DAY))", ttlDays))
	} else {
		sb.WriteString(fmt.Sprintf("\n) PRIMARY KEY (%s)", formatPrimaryKeys(metadata.PrimaryKeys)))
	}

	// Return the constructed Spanner CREATE TABLE query
	return sb.String(), nil
}

// Helper function to format primary keys
func formatPrimaryKeys(keys []string) string {
	// Remove parentheses from the keys
	for i, key := range keys {
		key = strings.Trim(key, "()") // Remove parentheses if they exist
		keys[i] = key
	}

	// Format the keys with backticks for Spanner
	return "`" + strings.Join(keys, "`, `") + "`"
}
