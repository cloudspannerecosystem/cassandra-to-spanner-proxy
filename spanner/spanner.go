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

package spanner

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	otelgo "github.com/ollionorg/cassandra-to-spanner-proxy/otel"
	"github.com/ollionorg/cassandra-to-spanner-proxy/responsehandler"
	"github.com/ollionorg/cassandra-to-spanner-proxy/tableConfig"
	"github.com/ollionorg/cassandra-to-spanner-proxy/utilities"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

const (
	spannerTSColumn               = "last_commit_ts"
	updateType                    = "update"
	insertType                    = "insert"
	deleteType                    = "delete"
	tsColumnParam                 = "tsValue"
	ifNotExistsColumnMetadataName = "[applied]"
	mapKeyPlaceholder             = "map_key"
)

var defaultCommitDelay = time.Duration(0) * time.Millisecond

var (
	appliedTrue  = message.Column{0x01}
	appliedFalse = message.Column{0x00}
)

var rowsResult = message.RowsResult{
	Metadata: &message.RowsMetadata{
		LastContinuousPage: true,
	},
}

const (
	InsertUpdateOrDeleteStatement = "Calling DML Statement"
	InsertOrUpdateMutation        = "Calling Insert or Update Using Mutations"
	DeleteUsingMutations          = "Calling Delete Using Mutations"
	FilterAndExecuteBatch         = "Filter And Execute Batch"
	partition_key                 = "partition_key"
	clustering                    = "clustering"
	regular                       = "regular"
)

type SpannerClientIface interface {
	SelectStatement(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error)
	InsertUpdateOrDeleteStatement(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error)
	Close() error
	InsertOrUpdateMutation(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error)
	DeleteUsingMutations(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error)
	UpdateMapByKey(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error)
	FilterAndExecuteBatch(ctx context.Context, queries []*responsehandler.QueryMetadata) (*message.RowsResult, error)
	GetTableConfigs(ctx context.Context, query responsehandler.QueryMetadata) (map[string]map[string]*tableConfig.Column, map[string][]tableConfig.Column, *SystemQueryMetadataCache, error)
}

type SpannerClient struct {
	Client           *spanner.Client
	Logger           *zap.Logger
	ResponseHandler  *responsehandler.TypeHandler
	KeyspaceFlatter  bool
	MaxCommitDelay   uint64
	ReplayProtection bool
}

// Response struct of filterBatch function
type FilterBatchResponse struct {
	Queries               []*responsehandler.QueryMetadata // Filtered batch queries
	CanExecuteAsBatch     bool                             // This flag is used to run all queries in sequence when batch contains map update by key
	OnlyInserts           bool                             // This flag is used to run all queries as mutations when only INSERTS are available in the batch
	CanExecuteAsMutations bool                             // This flag is used to run all queries as mutations when only INSERTS are available in the batch
}

// Close - Close the spanner client
func (sc *SpannerClient) Close() error {
	sc.Client.Close()
	return nil
}

// BuildCommitOptions returns the commit options for Spanner transactions.
func (sc *SpannerClient) BuildCommitOptions() spanner.CommitOptions {
	commitDelay := defaultCommitDelay
	if sc.MaxCommitDelay != 0 {
		commitDelay = time.Duration(sc.MaxCommitDelay) * time.Millisecond
	}
	return spanner.CommitOptions{
		MaxCommitDelay: &commitDelay,
	}
}

var NewSpannerClient = func(client *spanner.Client, logger *zap.Logger, responseHandler *responsehandler.TypeHandler, keyspaceFlatter bool, maxCommitDelay uint64, replayProtection bool) SpannerClientIface {
	return &SpannerClient{
		Client:           client,
		Logger:           logger,
		ResponseHandler:  responseHandler,
		KeyspaceFlatter:  keyspaceFlatter,
		MaxCommitDelay:   maxCommitDelay,
		ReplayProtection: replayProtection,
	}
}

type SystemQueryMetadataCache struct {
	KeyspaceSystemQueryMetadataCache map[primitive.ProtocolVersion][]message.Row
	TableSystemQueryMetadataCache    map[primitive.ProtocolVersion][]message.Row
	ColumnsSystemQueryMetadataCache  map[primitive.ProtocolVersion][]message.Row
}

// SelectStatement - Handle Select Operation
//
// Parameters:
//   - ctx: Context.
//   - query: query metadata.
//
// Returns: message.RowsResult (supported response format to the cassandra client) and error.
func (sc *SpannerClient) SelectStatement(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error) {
	var columnMetadata []*message.ColumnMetadata
	var data message.RowSet
	var err error
	var rowFuncs []func(int, *spanner.Row) ([]byte, error)
	var iter *spanner.RowIterator

	if query.ExecuteOptions.MaxStaleness > 0 {
		iter = sc.Client.Single().WithTimestampBound(spanner.MaxStaleness(time.Second*time.Duration(query.ExecuteOptions.MaxStaleness))).Query(ctx, *buildStmt(&query))
	} else {
		iter = sc.Client.Single().Query(ctx, *buildStmt(&query))
	}

	defer iter.Stop()

	rowCount := 0
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		if rowCount == 0 {
			otelgo.AddAnnotation(ctx, "Encoding Spanner Response")
			rowFuncs, columnMetadata, err = sc.ResponseHandler.BuildColumnMetadata(iter.Metadata.GetRowType(), query.ProtocalV, query.AliasMap, query.TableName, query.KeyspaceName)
			if err != nil {
				return nil, err
			}
		}

		mr, err := sc.ResponseHandler.BuildResponseRow(row, rowFuncs)
		if err != nil {
			return nil, err
		}
		data = append(data, mr)
		rowCount += 1
	}
	otelgo.AddAnnotationWithAttr(ctx, "Encoding Done", []attribute.KeyValue{
		attribute.Int("Row Count", rowCount),
	})
	if len(columnMetadata) == 0 {
		columnMetadata, err = sc.ResponseHandler.TableConfig.GetMetadataForSelectedColumns(query.TableName, query.SelectedColumns, query.AliasMap)
		if err != nil {
			sc.Logger.Error("error while fetching columnMetadata from config -", zap.Error(err))
			return nil, err
		}
	}
	result := &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(columnMetadata)),
			Columns:     columnMetadata,
		},
		Data: data,
	}
	return result, nil
}

// InsertUpdateOrDeleteStatement - Handle Insert/Update/Delete Operation
//
// Parameters:
//   - ctx: Context.
//   - query: query metadata.
//
// Returns: message.RowsResult (supported response format to the cassandra client) and error.
func (sc *SpannerClient) InsertUpdateOrDeleteStatement(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error) {
	otelgo.AddAnnotation(ctx, InsertUpdateOrDeleteStatement)
	_, err := sc.Client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, *buildStmt(&query))
		if err != nil {
			return err
		}
		return nil
	}, spanner.TransactionOptions{CommitOptions: sc.BuildCommitOptions()})

	return &rowsResult, err
}

// InsertOrUpdateMutation - Handle Insert/Update/Delete Operation
//
// Parameters:
//   - ctx: Context.
//   - query: query metadata.
//
// Returns: message.RowsResult (supported response format to the cassandra client) and error.
func (sc *SpannerClient) InsertOrUpdateMutation(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error) {
	otelgo.AddAnnotation(ctx, InsertOrUpdateMutation)
	if query.HasIfNotExists {
		return sc.InsertMutation(ctx, query)
	}

	if !sc.ReplayProtection && query.UsingTSCheck == "" {
		if _, err := sc.Client.Apply(ctx,
			[]*spanner.Mutation{buildInsertOrUpdateMutation(&query)},
			spanner.ApplyAtLeastOnce(),
			spanner.ApplyCommitOptions(sc.BuildCommitOptions())); err != nil {
			return nil, err
		}
		return &rowsResult, nil
	}

	_, err := sc.Client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		ignore := false
		var err error
		if query.UsingTSCheck != "" {
			stmt := spanner.Statement{
				SQL:    query.UsingTSCheck,
				Params: query.Params,
			}
			iter := txn.Query(ctx, stmt)
			ignore, err = ignoreInsert(iter)
			if err != nil {
				return err
			}
		}

		if !ignore {
			if err := txn.BufferWrite([]*spanner.Mutation{buildInsertOrUpdateMutation(&query)}); err != nil {
				return fmt.Errorf("failed to buffer write mutations: %w", err)
			}
		} else {
			sc.Logger.Debug("Insert operation ignored due to older timestamp")
		}
		return nil
	}, spanner.TransactionOptions{CommitOptions: sc.BuildCommitOptions()})

	return &rowsResult, err
}

// InsertOrUpdateMutation - Handle Insert Operation
//
// Parameters:
//   - ctx: Context.
//   - query: query metadata.
//
// Returns: message.RowsResult (supported response format to the cassandra client) and error.
func (sc *SpannerClient) InsertMutation(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error) {
	var err error
	if sc.ReplayProtection {
		_, err = sc.Client.Apply(ctx,
			[]*spanner.Mutation{buildInsertMutation(&query)},
			spanner.ApplyCommitOptions(sc.BuildCommitOptions()))
	} else {
		_, err = sc.Client.Apply(ctx,
			[]*spanner.Mutation{buildInsertMutation(&query)},
			spanner.ApplyAtLeastOnce(),
			spanner.ApplyCommitOptions(sc.BuildCommitOptions()))
	}

	row := appliedTrue
	if err != nil {
		row = appliedFalse
	}

	return &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: 1,
			Columns: []*message.ColumnMetadata{
				{
					Keyspace: query.KeyspaceName,
					Table:    query.TableName,
					Name:     ifNotExistsColumnMetadataName,
					Type:     datatype.Boolean,
				},
			},
		},
		Data: message.RowSet{message.Row{row}},
	}, nil
}

// Encode system_schema info to cassandra type
func (sc *SpannerClient) getSystemQueryMetadataCache(keyspaceMetadataRows, tableMetadataRows, columnsMetadataRows [][]interface{}) (*SystemQueryMetadataCache, error) {
	var err error
	protocolIV := primitive.ProtocolVersion4

	systemQueryMetadataCache := &SystemQueryMetadataCache{
		KeyspaceSystemQueryMetadataCache: make(map[primitive.ProtocolVersion][]message.Row),
		TableSystemQueryMetadataCache:    make(map[primitive.ProtocolVersion][]message.Row),
		ColumnsSystemQueryMetadataCache:  make(map[primitive.ProtocolVersion][]message.Row),
	}

	systemQueryMetadataCache.KeyspaceSystemQueryMetadataCache[protocolIV], err = sc.ResponseHandler.BuildResponseForSystemQueries(keyspaceMetadataRows, protocolIV)
	if err != nil {
		return systemQueryMetadataCache, err
	}
	systemQueryMetadataCache.TableSystemQueryMetadataCache[protocolIV], err = sc.ResponseHandler.BuildResponseForSystemQueries(tableMetadataRows, protocolIV)
	if err != nil {
		return systemQueryMetadataCache, err
	}
	systemQueryMetadataCache.ColumnsSystemQueryMetadataCache[protocolIV], err = sc.ResponseHandler.BuildResponseForSystemQueries(columnsMetadataRows, protocolIV)
	if err != nil {
		return systemQueryMetadataCache, err
	}

	return systemQueryMetadataCache, nil
}

// GetTableConfigs - Get Table config from Spanner.
//
// Parameters:
//   - ctx: Context.
//   - query: query metadata.
//
// Returns: []tableConfig.TableMetaData (metadata of each table) and error.
func (sc *SpannerClient) GetTableConfigs(ctx context.Context, query responsehandler.QueryMetadata) (map[string]map[string]*tableConfig.Column, map[string][]tableConfig.Column, *SystemQueryMetadataCache, error) {
	stmt := spanner.Statement{
		SQL: query.Query,
	}

	var kind string
	tableMetadata := make(map[string]map[string]*tableConfig.Column)
	pkMetadata := make(map[string][]tableConfig.Column)
	keyspaces := make(map[string]bool, 0)
	tables := make(map[string]bool, 0)
	columns := make(map[string]bool, 0)
	var systemQueryMetadataCache *SystemQueryMetadataCache

	keyspaceMetadataRows := [][]interface{}{}
	tableMetadataRows := [][]interface{}{}
	columnsMetadataRows := [][]interface{}{}

	iter := sc.Client.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, systemQueryMetadataCache, fmt.Errorf("resourceNotFoundException: %s", err)
		}

		column, err := extractColumnDataForTableConfig(row)
		if err != nil {
			return nil, nil, systemQueryMetadataCache, err
		}

		tableName := utilities.FlattenTableName(sc.KeyspaceFlatter, column.Metadata.Keyspace, column.Metadata.Table)

		if _, exists := tableMetadata[tableName]; !exists {
			tableMetadata[tableName] = make(map[string]*tableConfig.Column)
		}

		index := len(tableMetadata[tableName])

		if _, exists := pkMetadata[tableName]; !exists {
			var pkSlice []tableConfig.Column
			pkMetadata[tableName] = pkSlice
		}

		column.Metadata.Index = int32(index)
		tableMetadata[tableName][column.Name] = column
		if column.IsPrimaryKey {
			pkSlice := pkMetadata[tableName]
			pkSlice = append(pkSlice, *column)
			pkMetadata[tableName] = pkSlice
		}

		if _, exist := keyspaces[column.Metadata.Keyspace]; !exist {
			keyspaces[column.Metadata.Keyspace] = true
			keyspaceMetadataRows = append(keyspaceMetadataRows, []interface{}{
				column.Metadata.Keyspace, true, map[string]string{
					"class":              "org.apache.cassandra.locator.SimpleStrategy",
					"replication_factor": "1",
				},
			})
		}

		if _, exist := tables[column.Metadata.Table]; !exist {
			tables[column.Metadata.Table] = true
			tableMetadataRows = append(tableMetadataRows, []interface{}{
				column.Metadata.Keyspace, column.Metadata.Table, "99p", 0.01, map[string]string{
					"keys":               "ALL",
					"rows_per_partition": "NONE",
				},
				[]string{"compound"},
			})
		}
		uniqueColumnName := column.Metadata.Table + "_" + column.Metadata.Name
		if _, exist := columns[uniqueColumnName]; !exist {
			columns[uniqueColumnName] = true
			kind = regular
			if column.IsPrimaryKey {
				kind = clustering
				if column.PkPrecedence == 1 {
					kind = partition_key
				}
			}

			columnsMetadataRows = append(columnsMetadataRows, []interface{}{
				column.Metadata.Keyspace, column.Metadata.Table, column.Metadata.Name, "none", kind, 0, column.CQLType},
			)
		}
	}
	columnsMetadataRows = append(columnsMetadataRows, []interface{}{"system_schema", "columns", "kind", "none", partition_key, 0, "text"})

	if len(tableMetadata) < 1 {
		return nil, nil, systemQueryMetadataCache, fmt.Errorf("no rows found in config table (%s)", query.TableName)
	}

	systemQueryMetadataCache, err := sc.getSystemQueryMetadataCache(keyspaceMetadataRows, tableMetadataRows, columnsMetadataRows)
	if err != nil {
		return nil, nil, systemQueryMetadataCache, err
	}
	sc.ResponseHandler = &responsehandler.TypeHandler{
		Logger: sc.Logger,
		TableConfig: &tableConfig.TableConfig{
			Logger:          sc.Logger,
			TablesMetaData:  tableMetadata,
			PkMetadataCache: pkMetadata,
		},
	}
	return tableMetadata, pkMetadata, systemQueryMetadataCache, nil
}

// extractColumnDataForTableConfig - Extract Column data from Spanner row
//
// Parameters:
//   - row: Spanner Row.
//
// Returns: *tableConfig.Column (Column details for Table Config).
func extractColumnDataForTableConfig(row *spanner.Row) (*tableConfig.Column, error) {
	var columnName, columnType, tableName, keySpaceName string
	var isPrimaryKey bool
	var pkPrecedence int64

	if err := row.ColumnByName("ColumnName", &columnName); err != nil {
		return nil, err
	}
	if err := row.ColumnByName("ColumnType", &columnType); err != nil {
		return nil, err
	}
	if err := row.ColumnByName("TableName", &tableName); err != nil {
		return nil, err
	}
	if err := row.ColumnByName("KeySpaceName", &keySpaceName); err != nil {
		return nil, err
	}
	if err := row.ColumnByName("IsPrimaryKey", &isPrimaryKey); err != nil {
		return nil, err
	}
	if err := row.ColumnByName("PK_Precedence", &pkPrecedence); err != nil {
		return nil, err
	}

	keySpaceName = strings.ToLower(keySpaceName)
	tableName = strings.ToLower(tableName)
	columnName = strings.ToLower(columnName)
	columnType = strings.ToLower(columnType)
	cqlType, err := utilities.GetCassandraColumnType(columnType)
	if err != nil {
		return nil, err
	}

	columnMetadata := message.ColumnMetadata{
		Keyspace: keySpaceName,
		Table:    tableName,
		Name:     columnName,
		Type:     cqlType,
	}

	return &tableConfig.Column{
		Name:         columnName,
		CQLType:      columnType,
		IsPrimaryKey: isPrimaryKey,
		PkPrecedence: pkPrecedence,
		SpannerType:  utilities.GetSpannerColumnType(columnType),
		Metadata:     columnMetadata,
	}, nil

}

// ignoreInsert - Check if the insert operation should be ignored as per last commit timestamp.
//
// Parameters:
//   - ctx: Context.
//   - query: string
//   - params: map[string]interface{}
//
// Returns: (true if to be ignored or false) and error.

func ignoreInsert(iter *spanner.RowIterator) (bool, error) {
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return false, err
		}

		var tsVal time.Time
		if err := row.ColumnByName(spannerTSColumn, &tsVal); err != nil {
			return false, err
		} else {
			return true, nil
		}
	}
	return false, nil
}

// DeleteUsingMutations -Handle Delete Operation via Mutations API.
//
// Parameters:
//   - ctx: Context.
//   - query: query metadata.
//
// Returns: message.RowsResult (supported response format to the cassandra client) and error.

func (sc *SpannerClient) DeleteUsingMutations(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error) {
	otelgo.AddAnnotation(ctx, DeleteUsingMutations)

	var err error
	if sc.ReplayProtection {
		_, err = sc.Client.Apply(ctx,
			[]*spanner.Mutation{buildDeleteMutation(&query)},
			spanner.ApplyCommitOptions(sc.BuildCommitOptions()))
	} else {
		_, err = sc.Client.Apply(ctx,
			[]*spanner.Mutation{buildDeleteMutation(&query)},
			spanner.ApplyAtLeastOnce(),
			spanner.ApplyCommitOptions(sc.BuildCommitOptions()))
	}
	if err != nil {
		sc.Logger.Error("Error while Mutation Delete - "+query.Query, zap.Error(err))
		return nil, err
	}

	return &rowsResult, err
}

// UpdateMapByKey - Handle Update Map By Key Operation with pattern column[?] = true/false
// first select data from the table and then update
//
// Parameters:
//   - ctx: Context.
//   - query: query metadata.
//
// Returns: message.RowsResult (supported response format to the cassandra client) and error.
func (sc *SpannerClient) UpdateMapByKey(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error) {
	otelgo.AddAnnotation(ctx, "UpdateMapByKey")
	stmt := spanner.Statement{
		SQL:    query.SelectQueryMapUpdate,
		Params: query.Params,
	}

	_, err := sc.Client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		result, rowCount, err := executeQueryAndRetrieveResults(ctx, txn, stmt)
		if err != nil {
			sc.Logger.Error("Error executing query", zap.Error(err))
			return err
		}

		if rowCount == 1 {
			mapValue, err := decodeJSONData(result)
			if err != nil {
				sc.Logger.Error("Error decoding JSON data", zap.Error(err))
				return err
			}

			mapValue, err = updateMapWithNewValues(mapValue, &query)
			if err != nil {
				sc.Logger.Error("Error updating map with new values", zap.Error(err))
				return err
			}

			query.Params[mapKeyPlaceholder] = spanner.NullJSON{Value: mapValue, Valid: true}
			updatedRows, err := txn.Update(ctx, *buildStmt(&query))
			if err != nil {
				sc.Logger.Error("Error updating map with key/value", zap.String(query.Query, fmt.Sprintf("%s", query.Params)), zap.Error(err))
				return err
			}

			sc.Logger.Debug("Affected Records", zap.Int64("row_count", updatedRows))
			return nil
		} else if rowCount > 1 {
			return fmt.Errorf("more than a row is Updating at a time")
		}
		return nil
	}, spanner.TransactionOptions{CommitOptions: sc.BuildCommitOptions()})

	return &rowsResult, err
}

// executeQueryAndRetrieveResults - This function executes select statement, iterates rows and returns the results from Select Query.
//
// Parameters:
//   - ctx: Context.
//   - txn: Spanner Read/Write Txn
//   - stmt: Spanner Statement object
//
// Returns: spanner.NullJSON for map Column, rowCont and error.
func executeQueryAndRetrieveResults(ctx context.Context, txn *spanner.ReadWriteTransaction, stmt spanner.Statement) (spanner.NullJSON, int, error) {
	iter := txn.Query(ctx, stmt)
	defer iter.Stop()

	var result spanner.NullJSON
	var rowCount int

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return result, rowCount, err
		}
		err = row.Column(0, &result)
		if err != nil {
			return result, rowCount, err
		}
		rowCount++
	}

	return result, rowCount, nil
}

// decodeJSONData - This function decode the JSON data from spanner result and bind it to a map.
func decodeJSONData(result spanner.NullJSON) (map[string]bool, error) {
	var mapValue map[string]bool
	if err := json.Unmarshal([]byte(result.String()), &mapValue); err != nil {
		return nil, fmt.Errorf("error deserializing JSON to map[string]bool: %v", err)
	}
	return mapValue, nil
}

// updateMapWithNewValues - This function iterates through the map and update the key with the new value.
func updateMapWithNewValues(mapValue map[string]bool, query *responsehandler.QueryMetadata) (map[string]bool, error) {
	for _, setUpdate := range query.UpdateSetValues {
		if setUpdate.IsMapUpdateByKey {
			keyInterfaceVal, exists := query.Params[mapKeyPlaceholder]
			if !exists {
				return nil, fmt.Errorf(mapKeyPlaceholder + " not found in parameters")
			}
			key, ok := keyInterfaceVal.(string)
			if !ok {
				return nil, fmt.Errorf("error in decoding keyInterfaceVal to string")
			}
			valueStr, ok := setUpdate.RawValue.(string)
			if !ok {
				return nil, fmt.Errorf("error in casting RawValue to string - %s", setUpdate.RawValue)
			}
			valueBool, err := strconv.ParseBool(valueStr)
			if err != nil {
				return nil, fmt.Errorf("error while casting to boolean - %s", valueStr)
			}
			mapValue[key] = valueBool
		}
	}
	return mapValue, nil
}

// FilterAndExecuteBatch - Executes a read-write transaction on Spanner.
//
// Parameters:
//   - ctx: Context for the transaction.
//   - filteredResponse: Filtered Batch Metadata.
//
// Returns:
//   - Error if the transaction fails.

func (sc *SpannerClient) FilterAndExecuteBatch(ctx context.Context, queries []*responsehandler.QueryMetadata) (*message.RowsResult, error) {
	otelgo.AddAnnotation(ctx, FilterAndExecuteBatch)
	_, err := sc.Client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		filteredResponse, err := sc.filterBatch(ctx, txn, queries)
		if err != nil {
			return fmt.Errorf("failed to filter queries: %w", err)
		}

		sc.Logger.Debug("Check if batch has only inserts:", zap.Bool("onlyInserts", filteredResponse.OnlyInserts))
		sc.Logger.Debug("Number of queries after filtering the batch", zap.Int("count", len(filteredResponse.Queries)))

		if filteredResponse.CanExecuteAsBatch {
			// Execute all queries in a single transaction using BufferWrite and BatchUpdate.
			sc.Logger.Debug("Executing transaction via Batch")
			ms, stmts, err := sc.prepareQueriesAsBatch(ctx, txn, filteredResponse)
			if err != nil {
				return err
			}

			sc.Logger.Debug("Executing Mutations from the filtered batch", zap.Int("mutationCount", len(ms)))
			if len(ms) > 0 {
				if err := txn.BufferWrite(ms); err != nil {
					return fmt.Errorf("failed to buffer write mutations: %w", err)
				}
			}

			sc.Logger.Debug("Executing Statements from the filtered batch", zap.Int("statementCount", len(stmts)))
			if len(stmts) > 0 {
				rowCounts, err := txn.BatchUpdate(ctx, stmts)
				if err != nil {
					return fmt.Errorf("failed to execute batch update: %w", err)
				}
				otelgo.AddAnnotationWithAttr(ctx, "Executed Batch Update", []attribute.KeyValue{
					attribute.Int("Row Count", len(rowCounts)),
				})
			}

			return nil
		}

		// Execute all queries in a sequence via txn.Update function to manage complex update.
		sc.Logger.Debug("Executing transaction in sequence")
		err = sc.executeQueriesInSequence(ctx, txn, filteredResponse.Queries)
		if err != nil {
			return err
		}

		return nil
	}, spanner.TransactionOptions{CommitOptions: sc.BuildCommitOptions()})

	return &rowsResult, err
}

// prepareQueries - Prepares mutations and DML statements for the transaction based on the provided queries.
//
// Parameters:
//   - ctx: Transaction-specific context.
//   - txn: Spanner read-write transaction object.
//   - filteredResponse: Filtered Batch Metadata.
//
// Returns:
//   - Slice of Spanner mutations.
//   - Slice of Spanner DML statements.
//   - Error if the preparation fails.

func (sc *SpannerClient) prepareQueriesAsBatch(ctx context.Context, txn *spanner.ReadWriteTransaction, filteredResponse *FilterBatchResponse) ([]*spanner.Mutation, []spanner.Statement, error) {
	ms := []*spanner.Mutation{}
	stmts := []spanner.Statement{}

	for _, query := range filteredResponse.Queries {
		switch query.QueryType {
		case insertType:
			if (!filteredResponse.OnlyInserts && query.UsingTSCheck != "") || !filteredResponse.CanExecuteAsMutations {
				stmts = append(stmts, *buildStmt(query))
			} else {
				ms = append(ms, buildInsertOrUpdateMutation(query))
			}
		case deleteType:
			if filteredResponse.CanExecuteAsMutations && len(query.MutationKeyRange) > 0 {
				ms = append(ms, buildDeleteMutation(query))
			} else {
				stmts = append(stmts, *buildStmt(query))
			}
		case updateType:
			stmt, err := sc.prepareStatement(ctx, txn, query)
			if err != nil {
				return nil, nil, err
			}
			if stmt != nil {
				stmts = append(stmts, *stmt)
			}
		default:
			return nil, nil, fmt.Errorf("could not execute batch due to incorrect query type")
		}
	}

	return ms, stmts, nil
}

// executeQueriesInSequence - Executes mutations and DML statements for the transaction based on the sequence of the provided queries.
//
// Parameters:
//   - ctx: Transaction-specific context.
//   - txn: Spanner read-write transaction object.
//   - queries: Array of query metadata that dictates the SQL statements or mutations to prepare.
//
// Returns:
//   - Error if the preparation fails.

func (sc *SpannerClient) executeQueriesInSequence(ctx context.Context, txn *spanner.ReadWriteTransaction, queries []*responsehandler.QueryMetadata) error {
	ms := []*spanner.Mutation{}
	for _, query := range queries {
		switch query.QueryType {
		case insertType:
			if query.UsingTSCheck != "" {
				_, err := txn.Update(ctx, *buildStmt(query))
				if err != nil {
					return fmt.Errorf("failed to execute batch update: %w", err)
				}
			} else {
				ms = append(ms, buildInsertOrUpdateMutation(query))
			}
		case deleteType:
			if len(query.MutationKeyRange) > 0 {
				ms = append(ms, buildDeleteMutation(query))
			} else {
				_, err := txn.Update(ctx, *buildStmt(query))
				if err != nil {
					return fmt.Errorf("failed to execute batch update: %w", err)
				}
			}
		case updateType:
			stmt, err := sc.prepareStatement(ctx, txn, query)
			if err != nil {
				return err
			}
			if stmt != nil {
				_, err := txn.Update(ctx, *stmt)
				if err != nil {
					return fmt.Errorf("failed to execute batch update: %w", err)
				}
			}
		default:
			return fmt.Errorf("could not execute batch due to incorrect query type")
		}
	}

	if len(ms) > 0 {
		if err := txn.BufferWrite(ms); err != nil {
			return fmt.Errorf("failed to buffer write mutations: %w", err)
		}
	}

	return nil
}

// prepareStatement - Prepares a Spanner DML statement based on query metadata, especially for complex queries.
//
// Parameters:
//   - ctx: Transaction-specific context.
//   - txn: Spanner read-write transaction object.
//   - query: Single query metadata which may require special handling if it involves complex patterns.
//
// Returns:
//   - Prepared Spanner statement.
//   - Error if the preparation of the statement fails.

func (sc *SpannerClient) prepareStatement(ctx context.Context, txn *spanner.ReadWriteTransaction, query *responsehandler.QueryMetadata) (*spanner.Statement, error) {
	if query.SelectQueryMapUpdate != "" {
		return sc.updateMapByKeyInBatch(ctx, txn, query)
	}
	return buildStmt(query), nil
}

// buildStmt returns a Statement with the given SQL and Params.
func buildStmt(query *responsehandler.QueryMetadata) *spanner.Statement {
	return &spanner.Statement{
		SQL:    query.Query,
		Params: query.Params,
	}
}

// buildDeleteMutation returns a mutation object to remove the rows described by the KeySet from the table.
func buildDeleteMutation(query *responsehandler.QueryMetadata) *spanner.Mutation {
	return spanner.Delete(query.TableName, spanner.KeyRange{
		Start: query.MutationKeyRange,
		End:   query.MutationKeyRange,
		Kind:  spanner.ClosedClosed,
	})
}

// buildInsertMutation returns a mutation object to insert a row into a table.
func buildInsertMutation(query *responsehandler.QueryMetadata) *spanner.Mutation {
	return spanner.Insert(query.TableName, query.Paramkeys, query.ParamValues)
}

// buildInsertOrUpdateMutation returns a mutation object to insert a row into a table.
func buildInsertOrUpdateMutation(query *responsehandler.QueryMetadata) *spanner.Mutation {
	return spanner.InsertOrUpdate(query.TableName, query.Paramkeys, query.ParamValues)
}

type ErrorInfo struct {
	Query string `json:"query"`
	Error string `json:"error"`
}

// updateMapByKeyInBatch - Handle Complex Update Operation with pattern column_name[?] = true/false
// first select data from the table update data and then update
//
// Parameters:
//   - ctx: Context.
//   - txn *spanner.ReadWriteTransaction
//   - query: query metadata.
//
// Returns
//   - error
func (sc *SpannerClient) updateMapByKeyInBatch(ctx context.Context, txn *spanner.ReadWriteTransaction, query *responsehandler.QueryMetadata) (*spanner.Statement, error) {
	stmt := spanner.Statement{
		SQL:    query.SelectQueryMapUpdate,
		Params: query.Params,
	}

	result, rowCount, err := executeQueryAndRetrieveResults(ctx, txn, stmt)
	if err != nil {
		sc.Logger.Error("Error executing query", zap.Error(err))
		return nil, err
	}

	if rowCount == 1 {
		mapValue, err := decodeJSONData(result)
		if err != nil {
			sc.Logger.Error("Error decoding JSON data", zap.Error(err))
			return nil, err
		}

		mapValue, err = updateMapWithNewValues(mapValue, query)
		if err != nil {
			sc.Logger.Error("Error updating map with new values", zap.Error(err))
			return nil, err
		}

		query.Params[mapKeyPlaceholder] = spanner.NullJSON{Value: mapValue, Valid: true}
		return buildStmt(query), nil
	} else if rowCount > 1 {
		return nil, fmt.Errorf("complex Update does not support multiple row update in single query - %s", query.Query)
	} else {
		sc.Logger.Debug("no existing rows to perform map update by key")
		return nil, nil
	}
}

// filterBatch - This function filters out the irrelevant queries from batch based on USING TIMESTAMP
//
// Parameters:
//   - ctx: Context.
//   - queries []*responsehandler.QueryMetadata
//
// Returns
//   - error

func (sc *SpannerClient) filterBatch(ctx context.Context, txn *spanner.ReadWriteTransaction, queries []*responsehandler.QueryMetadata) (*FilterBatchResponse, error) {
	var nonFilterableQueries []*responsehandler.QueryMetadata
	var fileredBatch []*responsehandler.QueryMetadata
	canExecuteAsBatch := true
	onlyInserts := true
	canExecuteAsMutations := true
	distinctInserts := make(map[string]map[string][]*responsehandler.QueryMetadata)
	primaryKeyMap := make(map[string][]*responsehandler.QueryMetadata)

	// Iterate through all the queries in the Batch
	for _, query := range queries {
		_, ok := query.Params[spannerTSColumn]
		// Seperate out INSERTS WITH USING TIMESTAMP queries from the rest of the queries in the batch.
		if query.QueryType == insertType && ok {
			pkValues := make(map[string]interface{})
			for _, keys := range query.PrimaryKeys {
				pkValues[keys] = query.Params[keys]
			}
			jsonData, err := json.Marshal(pkValues)
			if err != nil {
				return nil, fmt.Errorf("error marshalling data to JSON: %w", err)
			}
			// Encode the JSON data to base64
			encodedKey := base64.StdEncoding.EncodeToString(jsonData)
			// Inserts the query into the map where key is encoded values of primary keys i.e each row
			primaryKeyMap[encodedKey] = append(primaryKeyMap[encodedKey], query)
			// each distinct encoded primary key values are mapped to its table names.
			distinctInserts[query.TableName] = primaryKeyMap
		} else {
			if query.SelectQueryMapUpdate != "" {
				canExecuteAsBatch = false
			}
			if canExecuteAsMutations && query.QueryType != insertType {
				canExecuteAsMutations = false
			}
			nonFilterableQueries = append(nonFilterableQueries, query)
			if onlyInserts {
				onlyInserts = false
			}
		}
	}

	// inserts executing on the same rows are sorted by their timestamps and only latest insert query is returned.
	sortedList, err := sortAndFilterUniqueTableNameAndPrimaryKeys(distinctInserts)
	if err != nil {
		return nil, err
	}

	// For each of the row inserts, a read query is executed to check if insert should be ignored based on TIMESTAMP
	for table, tableData := range sortedList {
		singleReadForInsertsWithTS, err := sc.singleReadForBatch(ctx, txn, tableData, table)
		if err != nil {
			return nil, err
		}
		for _, query := range singleReadForInsertsWithTS {
			if query != nil {
				fileredBatch = append(fileredBatch, query)
			}
		}
	}

	// non filterable queries are sorted in a manner where queries without USING TIMESTAMP will be executed before.
	sortedNonFilterableQueries := sortQueriesbyTS(nonFilterableQueries)

	// Merge filtered and non filtered queries.
	fileredBatch = append(fileredBatch, sortedNonFilterableQueries...)
	return &FilterBatchResponse{
		Queries:               fileredBatch,
		CanExecuteAsBatch:     canExecuteAsBatch,
		OnlyInserts:           onlyInserts,
		CanExecuteAsMutations: canExecuteAsMutations,
	}, nil
}

// sortAndFilterUniqueTableNameAndPrimaryKeys - This function filters the queries with similar primary key values and get the insert query with latest timestamp.
//
// Parameters:
//   - uniqueTablePk map[string]map[string][]*responsehandler.QueryMetadata
//
// Returns
//   map[string]map[string]*responsehandler.QueryMetadata, error

func sortAndFilterUniqueTableNameAndPrimaryKeys(uniqueTablePk map[string]map[string][]*responsehandler.QueryMetadata) (map[string]map[string]*responsehandler.QueryMetadata, error) {
	distinctList := make(map[string]map[string]*responsehandler.QueryMetadata)

	for table, tableData := range uniqueTablePk {
		singlePkData := make(map[string]*responsehandler.QueryMetadata)
		for pkStr, pkData := range tableData {
			if len(pkData) > 1 {
				latestTsQuery, err := filterByTimestamp(pkData)
				if err != nil {
					return nil, err
				}
				singlePkData[pkStr] = latestTsQuery

			} else if len(pkData) == 1 {
				singlePkData[pkStr] = pkData[0]
			}
		}
		distinctList[table] = singlePkData

	}

	return distinctList, nil
}

// singleReadForBatch - This function creates spanner keys for all primary key values in INSERTS and execute a single read to check commit TIMESTAMP.
//
// Parameters:
//   - ctx context.Context,
//   - pkData map[string]*responsehandler.QueryMetadata
// 	 - tableName string
//
// Returns
//   map[string]*responsehandler.QueryMetadata, error

func (sc *SpannerClient) singleReadForBatch(ctx context.Context, txn *spanner.ReadWriteTransaction, pkData map[string]*responsehandler.QueryMetadata, tableName string) (map[string]*responsehandler.QueryMetadata, error) {
	var pkList []string
	keys := make([]spanner.Key, 0, len(pkData))

	for _, query := range pkData {
		var vals []interface{}
		pks := query.PrimaryKeys
		for _, key := range pks {
			vals = append(vals, query.Params[key])
		}
		if len(pkList) < len(pks) {
			pkList = append(pkList[:0], pks...)
		}
		keys = append(keys, vals)
	}
	colList := append(pkList, spannerTSColumn)

	iter := txn.ReadWithOptions(ctx, tableName, spanner.KeySetFromKeys(keys...),
		colList, &spanner.ReadOptions{OrderBy: spannerpb.ReadRequest_ORDER_BY_NO_ORDER})
	return processRows(iter, pkData, colList)
}

// processRows - Process the rows fetched from single read.
//
// Parameters:
//   - iter *spanner.RowIterator
//   - pkData map[string]*responsehandler.QueryMetadata
//	 - colList []string
//
// Returns
//   map[string]*responsehandler.QueryMetadata, error

func processRows(iter *spanner.RowIterator, pkData map[string]*responsehandler.QueryMetadata, colList []string) (map[string]*responsehandler.QueryMetadata, error) {
	filteredResponse := make(map[string]interface{})
	var encodedKey string
	var timestamp *time.Time

	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if encodedKey, timestamp, err = processRow(row, colList); err != nil {
			return nil, err
		}
		filteredResponse[encodedKey] = timestamp
	}
	return buildResponse(pkData, filteredResponse)
}

// buildResponse - Build the final response for filterBatch
//
// Parameters:
//   - pkData map[string]*responsehandler.QueryMetadata
//	 - filteredResponse map[string]interface{}
//
// Returns
//   map[string]*responsehandler.QueryMetadata, error

func buildResponse(pkData map[string]*responsehandler.QueryMetadata, filteredResponse map[string]interface{}) (map[string]*responsehandler.QueryMetadata, error) {
	response := make(map[string]*responsehandler.QueryMetadata)

	for pk, query := range pkData {
		pkRow, ok := filteredResponse[pk]
		if !ok {
			response[pk] = query
		} else {
			if queryTs, ok := query.Params[spannerTSColumn].(*time.Time); ok {
				if dbTs, ok := pkRow.(*time.Time); ok {
					if queryTs.After(*dbTs) {
						response[pk] = query
					}
				} else {
					return nil, fmt.Errorf("expected last_commit_ts to be a time.Time value, got %T", pkRow)
				}
			} else {
				return nil, fmt.Errorf("expected query timestamp to be a time.Time value, got %T", query.Params[spannerTSColumn])
			}
		}
	}

	return response, nil
}

func processRow(row *spanner.Row, colList []string) (string, *time.Time, error) {
	var tsVal *time.Time
	if err := row.ColumnByName(spannerTSColumn, &tsVal); err != nil {
		return "", nil, fmt.Errorf("failed to get timestamp from row: %w", err)
	}

	data := make(map[string]interface{})
	for i, key := range colList {
		if key == spannerTSColumn {
			continue
		}
		val := row.ColumnValue(i).AsInterface()
		str, ok := val.(string)
		if ok {
			if num, err := strconv.ParseInt(str, 10, 64); err == nil {
				data[key] = num
			} else {
				data[key] = str
			}
		} else {
			data[key] = val
		}
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", nil, fmt.Errorf("error marshalling data to JSON: %w", err)
	}
	// Encode the JSON data to base64
	encodedKey := base64.StdEncoding.EncodeToString(jsonData)

	return encodedKey, tsVal, nil
}

// Function to find the latest last_commit_ts from an array of QueryMetadata
func filterByTimestamp(queryMetadatas []*responsehandler.QueryMetadata) (*responsehandler.QueryMetadata, error) {
	var latest *time.Time
	values := make(map[*time.Time]*responsehandler.QueryMetadata) // Zero value of time.Time is the zero timestamp
	var response *responsehandler.QueryMetadata

	for _, qm := range queryMetadatas {
		if ts, ok := qm.Params[spannerTSColumn]; ok {
			// Assert that the value is of type time.Time
			if timestamp, ok := ts.(*time.Time); ok {
				if latest == nil || timestamp.After(*latest) || timestamp.Equal(*latest) {
					latest = timestamp
					values[latest] = qm
				}
			} else {
				return nil, fmt.Errorf("last_commit_ts is not a time.Time value")
			}
		}
	}

	if latest.IsZero() {
		return nil, fmt.Errorf("no valid last_commit_ts found")
	} else {
		response = values[latest]
	}

	return response, nil
}

// Function to sort the slice based on timestamp
func sortQueriesbyTS(queries []*responsehandler.QueryMetadata) []*responsehandler.QueryMetadata {
	sort.Slice(queries, func(i, j int) bool {
		var ti, tj time.Time
		var okI, okJ bool

		if queries[i].QueryType == insertType {
			ti, okI = queries[i].Params[spannerTSColumn].(time.Time)
		} else {
			ti, okI = queries[i].Params[tsColumnParam].(time.Time)
		}

		if queries[j].QueryType == insertType {
			tj, okJ = queries[j].Params[spannerTSColumn].(time.Time)
		} else {
			tj, okJ = queries[j].Params[tsColumnParam].(time.Time)
		}

		if okI && okJ {
			return ti.Before(tj)
		}
		return okI && !okJ
	})

	return queries
}
