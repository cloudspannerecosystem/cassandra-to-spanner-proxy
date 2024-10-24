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
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
)

type Translator struct {
	Logger          *zap.Logger
	TableConfig     *tableConfig.TableConfig
	KeyspaceFlatter bool
	UseRowTimestamp bool
	UseRowTTL       bool
	Debug           bool
}

// SelectQueryMap represents the mapping of a select query along with its translation details.
type SelectQueryMap struct {
	CassandraQuery   string                               // Original query string
	SpannerQuery     string                               // Translated query string suitable for Spanner
	QueryType        string                               // Type of the query (e.g., SELECT)
	Table            string                               // Table involved in the query
	Keyspace         string                               // Keyspace to which the table belongs
	ColumnMeta       ColumnMeta                           // Translator generated Metadata about the columns involved
	Clauses          []Clause                             // List of clauses in the query
	Limit            Limit                                // Limit clause details
	OrderBy          OrderBy                              // Order by clause details
	Params           map[string]interface{}               // Parameters for the query
	ParamKeys        []string                             // column_name of the parameters
	AliasMap         map[string]tableConfig.AsKeywordMeta // Aliases used in the query
	PrimaryKeys      []string                             // Primary keys of the table
	ColumnsWithInOp  []string                             // Columns involved in IN operations
	ReturnMetadata   []*message.ColumnMetadata            // Metadata of selected columns in Cassandra format
	VariableMetadata []*message.ColumnMetadata            // Metadata of variable columns for prepared queries in Cassandra format
}

type OrderBy struct {
	isOrderBy bool
	Column    string
	Operation string
}

type Limit struct {
	isLimit bool
	Count   string
}

type ColumnMeta struct {
	Star   bool
	Column []tableConfig.SelectedColumns
}

type Column struct {
	Name        string
	SpannerType string
	CQLType     string
}

type Clause struct {
	Column       string
	Operator     string
	Value        string
	IsPrimaryKey bool
}

type ClauseResponse struct {
	Clauses   []Clause
	Params    map[string]interface{}
	ParamKeys []string
}

// InsertQueryMap represents the mapping of an insert query along with its translation details.
type InsertQueryMap struct {
	CassandraQuery   string                    // Original query string
	SpannerQuery     string                    // Translated query string suitable for Spanner
	QueryType        string                    // Type of the query (e.g., INSERT)
	Table            string                    // Table involved in the query
	Keyspace         string                    // Keyspace to which the table belongs
	Columns          []Column                  // List of columns involved in the insert operation
	Values           []interface{}             // Values to be inserted
	Params           map[string]interface{}    // Parameters for the query
	ParamKeys        []string                  // Column names of the parameters
	UsingTSCheck     string                    // Condition for using timestamp checks
	HasIfNotExists   bool                      // Condition for IF NOT EXISTS checks
	PrimaryKeys      []string                  // Primary keys of the table
	ReturnMetadata   []*message.ColumnMetadata // Metadata of all columns of that table in Cassandra format
	VariableMetadata []*message.ColumnMetadata // Metadata of variable columns for prepared queries in Cassandra format
}

type ColumnsResponse struct {
	Columns       []Column
	ValueArr      []string
	ParamKeys     []string
	PrimayColumns []string
}

// Delete Query Maps

// DeleteQueryMap represents the mapping of a delete query along with its translation details.
type DeleteQueryMap struct {
	CassandraQuery    string                    // Original query string
	SpannerQuery      string                    // Translated query string suitable for Spanner
	QueryType         string                    // Type of the query (e.g., DELETE)
	Table             string                    // Table involved in the query
	Keyspace          string                    // Keyspace to which the table belongs
	Clauses           []Clause                  // List of clauses in the delete query
	Params            map[string]interface{}    // Parameters for the query
	ParamKeys         []string                  // Column names of the parameters
	PrimaryKeys       []string                  // Primary keys of the table
	ReturnMetadata    []*message.ColumnMetadata // Metadata of all columns of that table in Cassandra format
	VariableMetadata  []*message.ColumnMetadata // Metadata of variable columns for prepared queries in Cassandra format
	ExecuteByMutation bool                      // Flag to indicate if the delete should be executed by mutation
}

// Update Query Map
// UpdateQueryMap represents the mapping of an update query along with its translation details.
type UpdateQueryMap struct {
	CassandraQuery       string                    // Original query string
	SpannerQuery         string                    // Translated query string suitable for Spanner
	QueryType            string                    // Type of the query (e.g., UPDATE)
	Table                string                    // Table involved in the query
	Keyspace             string                    // Keyspace to which the table belongs
	UpdateSetValues      []UpdateSetValue          // Values to be updated
	Clauses              []Clause                  // List of clauses in the update query
	Params               map[string]interface{}    // Parameters for the query
	ParamKeys            []string                  // Column names of the parameters
	PrimaryKeys          []string                  // Primary keys of the table                   // Flag to indicate if local IDs pattern is used
	ReturnMetadata       []*message.ColumnMetadata // Metadata of all columns of that table in Cassandra format
	VariableMetadata     []*message.ColumnMetadata // Metadata of variable columns for prepared queries in Cassandra format
	SelectQueryMapUpdate string                    // Select query to pull the map information to update the specific key/value
	MapUpdateColumn      string                    // Column name to update map based on key/value.
}
type UpdateSetValue struct {
	Column           string
	Value            string
	IsMapUpdateByKey bool
	MapUpdateKey     string
	RawValue         interface{}
}

type UpdateSetResponse struct {
	UpdateSetValues []UpdateSetValue
	ParamKeys       []string
	Params          map[string]interface{}
	MapUpdateColumn string
}

type TableObj struct {
	TableName    string
	KeyspaceName string
}
