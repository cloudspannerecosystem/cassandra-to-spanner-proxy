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
	"embed"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"cloud.google.com/go/spanner/spansql"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/responsehandler"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/translator"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

var (
	INSERT_COMM_UPLOAD                  = "INSERT OR UPDATE INTO  keyspace1_comm_upload_event (`user_id`, `upload_time`, `comm_event`, `spanner_ttl_ts`, `last_commit_ts`)  values(@user_id, @upload_time,@comm_event,@ttl_ts, @last_commit_ts )"
	DELETE_COMM_UPLOAD_RANGE            = "DELETE FROM keyspace1_comm_upload_event WHERE `last_commit_ts` <= @tsValue AND `user_id` = @value1"
	DELETE_COMM_UPLOAD_RANGE_WITHOUT_TS = "DELETE FROM keyspace1_comm_upload_event WHERE`user_id` = @value1"
	DELETE_COMM_UPLOAD                  = "DELETE FROM keyspace1_comm_upload_event WHERE `last_commit_ts` <= @tsValue AND `user_id` = @value1 and `upload_time` = @value2"
	DELETE_ADDRESS_BOOK                 = "DELETE FROM keyspace1_address_book_entries WHERE `last_commit_ts` <= @tsValue AND `user_id` = @value1"
	Table_COMM_UPLOAD                   = "keyspace1_comm_upload_event"
	KEYSPACE                            = "keyspace1"
	QUERY_TYPE_INSERT                   = "insert"
	QUERY_TYPE_DELETE                   = "delete"
	QUERY_TYPE_UPDATE                   = "update"
	ErrorWhileFilterAndExecuteBatch     = "Error while executing FilterAndExecuteBatch : %v"
	ErrorWhileFilterBatch               = "Error while executing FilterBatch : %v"
	UsingTsCheckQueryCommUpload         = "SELECT last_commit_ts FROM keyspace1_comm_upload_event WHERE `user_id` = @user_id AND `upload_time` = @upload_time AND `last_commit_ts` > @last_commit_ts"
	LAST_COMMIT_TS                      = "last_commit_ts"
	TTL_TS                              = "ttl_ts"
	TABLE_ADDRESS                       = "keyspace1_address_book_entries"
)

const (
	NotFoundOnSpanner             = "Inserted Rows not found in spanner"
	ErrorWhileFetchingFromSpanner = "Error while fetching data from Spanner: %v"
	largestTimeStampMismatchError = "largest timestamp is not matching"
)

var SCHEMAFILE embed.FS

var TEST_NETWORK = "cassandra-spanner-proxy-test"

var logger = zap.NewNop()

var (
	Project  = "cassandra-to-spanner"
	Instance = "spanner-instance-dev"
	Database = "cluster1"
)

type Emulator struct {
	testcontainers.Container
	Endpoint string
	Project  string
	Instance string
	Database string
}

type Service struct {
	testcontainers.Container
	Endpoint string
}

func teardown(ctx context.Context, emulator *Emulator) {
	err := emulator.Terminate(ctx)
	if err != nil {
		fmt.Println("Error while terminating emulator - ", err.Error())
		os.Exit(0)
	}
}

func setupSpannerEmulator(ctx context.Context) (*Emulator, error) {
	req := testcontainers.ContainerRequest{
		Image:        "gcr.io/cloud-spanner-emulator/emulator:latest",
		ExposedPorts: []string{"9010/tcp"},
		Networks: []string{
			TEST_NETWORK,
		},
		NetworkAliases: map[string][]string{
			TEST_NETWORK: {
				"emulator",
			},
		},
		Name:       "emulator",
		WaitingFor: wait.ForLog("gRPC server listening at").WithStartupTimeout(time.Minute * 2),
	}
	spannerEmulator, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	// Retrieve the container IP
	ip, err := spannerEmulator.Host(ctx)
	if err != nil {
		return nil, err
	}

	// Retrieve the container port
	port, err := spannerEmulator.MappedPort(ctx, "9010")
	if err != nil {
		return nil, err
	}

	// OS environment needed for setting up instance and database
	os.Setenv("SPANNER_EMULATOR_HOST", fmt.Sprintf("%s:%d", ip, port.Int()))
	fmt.Println("SPANNER_EMULATOR_HOST ->", fmt.Sprintf("%s:%d", ip, port.Int()))

	var ec = Emulator{
		Container: spannerEmulator,
		Endpoint:  "emulator:9010",
		Project:   "cassandra-to-spanner",
		Instance:  "spanner-instance-dev",
		Database:  "cluster1",
	}

	// Create instance
	err = setupInstance(ctx, ec)
	if err != nil {
		return nil, err
	}

	// Define the database and schema
	err = setupDatabase(ctx, ec)
	if err != nil {
		return nil, err
	}

	return &ec, nil
}

// loadTestData connects to a Spanner database and executes a series of insert queries.
func loadTestData(ctx context.Context, ec Emulator) error {
	// Create a client for the Spanner database
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", ec.Project, ec.Instance, ec.Database))
	if err != nil {
		return err
	}
	defer client.Close()

	insertQueries, err := parseDMLFile("../test_data/dml.sql")
	if err != nil {
		return err
	}

	// Begin a write transaction and use BatchUpdate
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Prepare the batch of statements
		var stmts []spanner.Statement
		for _, query := range insertQueries {
			stmts = append(stmts, spanner.Statement{SQL: query})
		}

		// Execute the batch of statements
		_, err := txn.BatchUpdate(ctx, stmts)
		if err != nil {
			return err
		}

		return nil // Commit the transaction
	})
	return err
}

// parseDDLFile reads a DDL file and returns the DDL statements as a slice of strings.
func parseDDLFile(fileName string) ([]string, error) {
	// Read the file contents
	content, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %v", err)
	}

	// Parse the DDL statements
	statements, err := spansql.ParseDDL(fileName, string(content))
	if err != nil {
		return nil, fmt.Errorf("failed to parse DDL: %v", err)
	}

	// Convert the statements to a slice of strings
	var ddlStatements []string
	for _, stmt := range statements.List {
		ddlStatements = append(ddlStatements, stmt.SQL())
	}

	return ddlStatements, nil
}

// parseDDLFile reads a DDL file and returns the DDL statements as a slice of strings.
func parseDMLFile(fileName string) ([]string, error) {
	// Read the file contents
	content, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %v", err)
	}

	// Parse the DDL statements
	statements, err := spansql.ParseDML(fileName, string(content))
	if err != nil {
		return nil, fmt.Errorf("failed to parse DML: %v", err)
	}

	// Convert the statements to a slice of strings
	var dmlStatements []string
	for _, stmt := range statements.List {
		dmlStatements = append(dmlStatements, stmt.SQL())
	}

	return dmlStatements, nil
}

func setupDatabase(ctx context.Context, ec Emulator) error {
	schemaStatements, err := parseDDLFile("../test_data/schema.sql")
	if err != nil {
		return err
	}

	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer adminClient.Close()

	op, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", ec.Project, ec.Instance),
		CreateStatement: "CREATE DATABASE `" + ec.Database + "`",
		ExtraStatements: schemaStatements,
	})
	if err != nil {
		fmt.Printf("Error: [%s]", err)
		return err
	}
	if _, err := op.Wait(ctx); err != nil {
		fmt.Printf("Error: [%s]", err)
		return err
	}
	err = loadTestData(ctx, ec)
	if err != nil {
		fmt.Printf("Error while insert: [%s]", err)
		return err
	}

	fmt.Printf("Created emulator database [%s]\n", ec.Database)
	return nil

}

func setupInstance(ctx context.Context, ec Emulator) error {
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	defer instanceAdmin.Close()

	op, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", ec.Project),
		InstanceId: ec.Instance,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/%s", ec.Project, "emulator-config"),
			DisplayName: ec.Instance,
			NodeCount:   1,
		},
	})
	if err != nil {
		return fmt.Errorf("could not create instance %s: %v", fmt.Sprintf("projects/%s/instances/%s", ec.Project, ec.Instance), err)
	}
	// Wait for the instance creation to finish.
	i, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for instance creation to finish failed: %v", err)
	}

	// The instance may not be ready to serve yet.
	if i.State != instancepb.Instance_READY {
		fmt.Printf("instance state is not READY yet. Got state %v\n", i.State)
	}
	fmt.Printf("Created emulator instance [%s]\n", ec.Instance)

	return nil
}

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	// Setup the docker network so containers can talk to each other
	net, err := network.New(ctx,
		network.WithAttachable(), // Make the network attachable
	)
	if err != nil {
		fmt.Println("Error creating network - ", err.Error())
		os.Exit(1)
	}
	defer func() {
		err := net.Remove(ctx)
		if err != nil {
			fmt.Println("Error at net.Remove - ", err.Error())
		}
	}()

	// Setup the emulator container and default instance/database
	spannerEmulator, err := setupSpannerEmulator(ctx)
	if err != nil {
		fmt.Printf("Error setting up emulator: %s\n", err)
		os.Exit(1)
	}

	defer teardown(ctx, spannerEmulator)

	os.Exit(m.Run())
}

func connectToSpanner(t *testing.T) (*spanner.Client, context.Context) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", Project, Instance, Database))
	if err != nil {
		t.Error("Error while getting spanner connection")
		return nil, ctx
	}
	return client, ctx
}

func closeSpannerConnection(session *spanner.Client) {
	session.Close()
}

func TestBasicQueries(t *testing.T) {
	session, ctx := connectToSpanner(t)
	defer closeSpannerConnection(session)
	query := "select * from TableConfigurations limit 5;"

	// Execute the query
	iter := session.Single().Query(ctx, spanner.Statement{SQL: query})
	defer iter.Stop()
	result := make([]interface{}, 0)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		result = append(result, row)
	}
	if len(result) == 0 {
		t.Errorf("Expected more than 0, rows but found %d rows", len(result))
	}

}

func getSpannerClient(t *testing.T) (*SpannerClient, context.Context, error) {
	client, ctx := connectToSpanner(t)
	return &SpannerClient{Client: client, Logger: logger, ResponseHandler: getResponseHandler(), KeyspaceFlatter: true}, ctx, nil
}

func mockQueryMetadata() responsehandler.QueryMetadata {
	return responsehandler.QueryMetadata{
		Query:           "SELECT user_id, upload_time, comm_event FROM keyspace1_comm_upload_event WHERE user_id = 'some_user_id' ",
		TableName:       Table_COMM_UPLOAD,
		KeyspaceName:    KEYSPACE,
		ProtocalV:       4,
		SelectedColumns: []tableConfig.SelectedColumns{{Name: "user_id"}, {Name: "upload_time"}, {Name: "comm_event"}},
	}
}

func getTableConfig() *tableConfig.TableConfig {

	return &tableConfig.TableConfig{
		Logger:          logger,
		KeyspaceFlatter: true,
		PkMetadataCache: map[string][]tableConfig.Column{
			Table_COMM_UPLOAD: {
				{
					Name:         "user_id",
					CQLType:      "text",
					IsPrimaryKey: true,
					PkPrecedence: 1,
					Metadata: message.ColumnMetadata{
						Keyspace: KEYSPACE,
						Table:    Table_COMM_UPLOAD,
						Name:     "user_id",
						Index:    1,
						Type:     datatype.Varchar}},
				{
					Name:         "upload_time",
					CQLType:      "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 2,
					Metadata: message.ColumnMetadata{
						Keyspace: KEYSPACE,
						Table:    Table_COMM_UPLOAD,
						Name:     "upload_time",
						Index:    2,
						Type:     datatype.Bigint,
					},
				},
			},
			TABLE_ADDRESS: {
				{
					Name:         "user_id",
					CQLType:      "text",
					IsPrimaryKey: true,
					PkPrecedence: 1,
					Metadata: message.ColumnMetadata{
						Keyspace: KEYSPACE,
						Table:    TABLE_ADDRESS,
						Name:     "user_id",
						Index:    1,
						Type:     datatype.Varchar,
					},
				},
			},
		},
		TablesMetaData: map[string]map[string]*tableConfig.Column{
			Table_COMM_UPLOAD: {
				"user_id": &tableConfig.Column{
					Name:         "user_id",
					CQLType:      "text",
					IsPrimaryKey: true,
					PkPrecedence: 1,
					Metadata: message.ColumnMetadata{
						Keyspace: KEYSPACE,
						Table:    Table_COMM_UPLOAD,
						Name:     "user_id",
						Index:    1,
						Type:     datatype.Varchar,
					},
				},
				"upload_time": &tableConfig.Column{
					Name:         "upload_time",
					CQLType:      "bigint",
					IsPrimaryKey: true,
					PkPrecedence: 2,
					Metadata: message.ColumnMetadata{
						Keyspace: KEYSPACE,
						Table:    Table_COMM_UPLOAD,
						Name:     "upload_time",
						Index:    2,
						Type:     datatype.Bigint,
					},
				},
				"comm_event": &tableConfig.Column{
					Name:         "comm_event",
					CQLType:      "blob",
					IsPrimaryKey: false,
					PkPrecedence: 0,
					Metadata: message.ColumnMetadata{
						Keyspace: KEYSPACE,
						Table:    Table_COMM_UPLOAD,
						Name:     "comm_event",
						Index:    0,
						Type:     datatype.Blob,
					},
				},
			},
			TABLE_ADDRESS: {
				"user_id": &tableConfig.Column{
					Name:         "user_id",
					CQLType:      "text",
					IsPrimaryKey: true,
					PkPrecedence: 1,
					Metadata: message.ColumnMetadata{
						Keyspace: KEYSPACE,
						Table:    TABLE_ADDRESS,
						Name:     "user_id",
						Index:    1,
						Type:     datatype.Varchar,
					},
				},
				"test_ids": &tableConfig.Column{
					Name:         "test_ids",
					CQLType:      "text",
					IsPrimaryKey: false,
					PkPrecedence: 0,
					Metadata: message.ColumnMetadata{
						Keyspace: KEYSPACE,
						Table:    TABLE_ADDRESS,
						Name:     "test_ids",
						Index:    0,
						Type:     datatype.NewMapType(datatype.Varchar, datatype.Boolean),
					},
				},
			},
		}}
}

func getResponseHandler() *responsehandler.TypeHandler {
	tableConfig := getTableConfig()
	return &responsehandler.TypeHandler{
		Logger:      logger,
		TableConfig: tableConfig,
	}
}

func TestSelectStatement(t *testing.T) {
	sc, ctx, _ := getSpannerClient(t)
	defer sc.Close()

	query := mockQueryMetadata() // Assumes mockQueryMetadata is appropriately defined elsewhere
	result, err := sc.SelectStatement(ctx, query)
	if err != nil {
		t.Errorf("Unexpected error in select query: %v", err)
	}
	assert.Equal(t, 1, len(result.Data), "there should not be any rows in result")
}

func TestSelectStatementStale(t *testing.T) {
	sc, ctx, _ := getSpannerClient(t)
	defer sc.Close()

	query := mockQueryMetadata() // Assumes mockQueryMetadata is appropriately defined elsewhere
	result, err := sc.SelectStatement(ctx, query)
	if err != nil {
		t.Errorf("Unexpected error in select query: %v", err)
	}
	assert.Equal(t, 1, len(result.Data), "there should not be any rows in result")
}

func TestClose(t *testing.T) {
	sc, _, _ := getSpannerClient(t)
	// Attempt to close and check for errors
	if err := sc.Close(); err != nil {
		t.Errorf("Error while testing Spanner close connection functionality - %s", err)
	}
}

func TestInsertUpdateOrDeleteStatement(t *testing.T) {
	sc, ctx, _ := getSpannerClient(t)
	defer sc.Close()
	testCases := []struct {
		name        string
		query       responsehandler.QueryMetadata
		expectError bool
	}{
		{
			name: "ValidDeleteOperation",
			query: responsehandler.QueryMetadata{
				Query:        DELETE_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_DELETE,
				Params:       map[string]interface{}{"tsValue": time.Now(), "value1": "user_id", "value2": 100},
			},
			expectError: false,
		},
		{
			name: "ValidUpdateOperation",
			query: responsehandler.QueryMetadata{
				Query:        "UPDATE keyspace1_comm_upload_event SET  `comm_event` = @set1 , `last_commit_ts` = PENDING_COMMIT_TIMESTAMP() WHERE `user_id` = @value1 ",
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_UPDATE,
				Params:       map[string]interface{}{"tsValue": time.Now(), "value1": "user_id", "set1": []byte("ssdddd")},
			},
			expectError: false,
		},
		{
			name: "ValidInsertOperation",
			query: responsehandler.QueryMetadata{
				Query:        "insert into  keyspace1_comm_upload_event (`user_id`, `upload_time`, `comm_event`, `spanner_ttl_ts`, `last_commit_ts`)  values(@user_id, @upload_time,@comm_event,@ttl_ts, @tsValue ) ;",
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params:       map[string]interface{}{"tsValue": time.Now(), TTL_TS: time.Now().Add(time.Second * 5), "user_id": "user_id", "upload_time": 423545, "comm_event": []byte("ssdddd")},
			},
			expectError: false,
		},
		{
			name: "ErrorWrongTableName",
			query: responsehandler.QueryMetadata{
				Query:        "DELETE FROM wrong_table_name WHERE `last_commit_ts` <= @tsValue AND `user_id` = @value1 AND `upload_time` = @value2; ",
				TableName:    "wrong_table_name",
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_DELETE,
				Params:       map[string]interface{}{"tsValue": time.Now(), "value1": "user_id", "value2": 100},
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := sc.InsertUpdateOrDeleteStatement(ctx, tc.query)
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error but got nil for test case: %s", tc.name)
				}
			} else {
				if err != nil {
					t.Errorf("Error in InsertUpdateOrDeleteStatement for test case %s: %s", tc.name, err.Error())
				}
				assert.Equal(t, 0, len(result.Data), "unexpected number of rows in result for test case: "+tc.name)
			}
		})
	}
}

// getDataFromSpanner retrieves data from a Spanner table and returns it as a slice of maps.
func getDataFromSpanner(ctx context.Context, client *spanner.Client, query string) ([]map[string]interface{}, error) {
	stmt := spanner.NewStatement(query)
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var results []map[string]interface{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		columnNames := row.ColumnNames()
		result := make(map[string]interface{})

		for i, columnName := range columnNames {
			var col interface{}
			columnType := row.ColumnType(i)
			switch columnType.Code {
			case spannerpb.TypeCode_STRING:
				var v string
				if err := row.Column(i, &v); err != nil {
					return nil, err
				}
				col = v
			case spannerpb.TypeCode_INT64:
				var v int64
				if err := row.Column(i, &v); err != nil {
					return nil, err
				}
				col = v
			case spannerpb.TypeCode_FLOAT64:
				var v float64
				if err := row.Column(i, &v); err != nil {
					return nil, err
				}
				col = v
			case spannerpb.TypeCode_BOOL:
				var v bool
				if err := row.Column(i, &v); err != nil {
					return nil, err
				}
				col = v
			case spannerpb.TypeCode_BYTES:
				var v []byte
				if err := row.Column(i, &v); err != nil {
					return nil, err
				}
				col = v
			case spannerpb.TypeCode_TIMESTAMP:
				var v spanner.NullTime
				if err := row.Column(i, &v); err != nil {
					return nil, err
				}
				if v.Valid {
					col = v.Time
				} else {
					col = nil
				}
			case spannerpb.TypeCode_JSON:
				var v spanner.NullJSON
				if err := row.Column(i, &v); err != nil {
					return nil, err
				}
				if v.Valid {
					col = v.Value
				} else {
					col = nil
				}
			default:
				var v interface{}
				if err := row.Column(i, &v); err != nil {
					return nil, err
				}
				col = v
			}

			result[columnName] = col
		}

		results = append(results, result)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no rows found")
	}

	return results, nil
}

func TestInsertOrUpdateMutation(t *testing.T) {
	sc, ctx, _ := getSpannerClient(t)
	defer sc.Close()
	testCases := []struct {
		name              string
		timestamp         time.Time
		usingTSCheck      string
		params            map[string]interface{}
		shouldGetInserted bool
	}{
		{
			name:         "CurrentTimestamp",
			timestamp:    time.Now().Add(time.Millisecond * -10),
			usingTSCheck: UsingTsCheckQueryCommUpload,
			params: map[string]interface{}{
				"comm_event":   []byte("sample_byte"),
				LAST_COMMIT_TS: time.Now().Add(time.Millisecond * -10),
				"upload_time":  24235342,
				"user_id":      "0856e83c-81c3-430c-b018-9eb22eaX",
			},
			shouldGetInserted: true,
		},
		{
			name:         "PastTimestamp",
			timestamp:    time.Now().Add(-2 * time.Second),
			usingTSCheck: UsingTsCheckQueryCommUpload,
			params: map[string]interface{}{
				"comm_event":   []byte("sample_byteXYZ"),
				LAST_COMMIT_TS: time.Now().Add(-2 * time.Second),
				"upload_time":  24235342,
				"user_id":      "0856e83c-81c3-430c-b018-9eb22eaX",
			},
			shouldGetInserted: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query := responsehandler.QueryMetadata{
				Query:        "INSERT OR UPDATE INTO keyspace1_comm_upload_event (`user_id`, `upload_time`, `comm_event`, `last_commit_ts`) VALUES (@user_id, @upload_time, @comm_event, @commitTimestamp);",
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{tc.params["user_id"], tc.params["upload_time"], tc.params["comm_event"], tc.params[LAST_COMMIT_TS]},
				UsingTSCheck: tc.usingTSCheck,
				Params:       tc.params,
			}
			result, err := sc.InsertOrUpdateMutation(ctx, query)
			if err != nil {
				t.Errorf("Error in InsertOrUpdateMutation: %s", err.Error())
			}
			assert.Equal(t, 0, len(result.Data), "unexpected number of rows in result")
			data, err := getDataFromSpanner(ctx, sc.Client, fmt.Sprintf("select * from keyspace1_comm_upload_event where user_id='%s';", tc.params["user_id"]))
			if err != nil {
				t.Errorf(ErrorWhileFetchingFromSpanner, err)
			}
			if len(data) == 0 {
				t.Error(NotFoundOnSpanner)
			}
			if val, ok := data[0]["user_id"].(string); ok {
				assert.Equal(t, val, tc.params["user_id"], fmt.Sprintf("expected %s but received %s", tc.params["user_id"], val))
			} else {
				t.Errorf("unable to cast user_id %s", data[0]["user_id"])
			}
			if val, ok := data[0]["upload_time"].(int64); ok {
				if uploadTime, ok := tc.params["upload_time"].(int64); ok {
					assert.Equal(t, val, uploadTime, fmt.Sprintf("expected %d but received %d", tc.params["upload_time"], val))

				}
			} else {
				t.Errorf("unable to cast upload_time %s", data[0]["upload_time"])
			}
			if val, ok := data[0]["comm_event"].([]byte); ok {
				if tc.shouldGetInserted {
					assert.Equal(t, val, tc.params["comm_event"], fmt.Sprintf("expected %d but received %d", tc.params["comm_event"], val))
				} else {
					assert.NotEqualValues(t, val, tc.params["comm_event"], fmt.Sprintf("expected %d but received %d", tc.params["comm_event"], val))
				}
			} else {
				t.Errorf("unable to cast comm_event %s", data[0]["comm_event"])
			}
		})
	}
}

func TestGetTableConfigs(t *testing.T) {
	sc, ctx, _ := getSpannerClient(t)
	defer sc.Close()
	// Set up test cases
	testCases := []struct {
		name         string
		query        responsehandler.QueryMetadata
		expectError  bool
		errorMessage string
	}{
		{
			name:        "SuccessfulDataRetrieval",
			query:       responsehandler.QueryMetadata{Query: "SELECT * FROM TableConfigurations ORDER BY TableName, IsPrimaryKey, PK_Precedence", TableName: "TableConfigurations"},
			expectError: false,
		},
		{
			name:         "NoRowsFound",
			query:        responsehandler.QueryMetadata{Query: "SELECT * FROM TableConfigurations where TableName='XYZ' ORDER BY TableName, IsPrimaryKey, PK_Precedence", TableName: "TableConfigurations"},
			expectError:  true,
			errorMessage: "no rows found in config table (TableConfigurations)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableMetadata, pkMetadata, systemQueryMetadataCache, err := sc.GetTableConfigs(ctx, tc.query)
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected an error but got none")
				} else if err.Error() != tc.errorMessage {
					t.Errorf("Expected error message '%s', got '%s'", tc.errorMessage, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if len(tableMetadata) < 1 {
					t.Error("Expected at least one result, got none")
				}
				if len(pkMetadata) < 1 {
					t.Error("Expected at least one result, got none")
				}
				if len(systemQueryMetadataCache.KeyspaceSystemQueryMetadataCache[primitive.ProtocolVersion4]) < 1 {
					t.Error("Expected at least one result for systemQueryMetadataCache.KeyspaceSystemQueryMetadataCache, got none")
				}
				if len(systemQueryMetadataCache.TableSystemQueryMetadataCache[primitive.ProtocolVersion4]) < 1 {
					t.Error("Expected at least one result for systemQueryMetadataCache.TableSystemQueryMetadataCache, got none")
				}
				if len(systemQueryMetadataCache.ColumnsSystemQueryMetadataCache[primitive.ProtocolVersion4]) < 1 {
					t.Error("Expected at least one result for systemQueryMetadataCache.ColumnsSystemQueryMetadataCache, got none")
				}
			}
		})
	}
}

func TestDeleteUsingMutations(t *testing.T) {
	sc, ctx, _ := getSpannerClient(t)
	defer sc.Close()
	query := responsehandler.QueryMetadata{
		Query:        "DELETE FROM keyspace1_comm_event_uploads WHERE `user_id` = @value1 ; ",
		TableName:    Table_COMM_UPLOAD,
		KeyspaceName: KEYSPACE,
		ProtocalV:    4,
		QueryType:    QUERY_TYPE_DELETE,
		Params:       map[string]interface{}{"value1": "0856e83c-81c3-430c-b018-9eb22ea3882a"},
	}

	_, err := sc.DeleteUsingMutations(ctx, query)
	if err != nil {
		t.Error("Error while function call DeleteUsingMutations")
	}
}

func TestUpdateMapByKey(t *testing.T) {
	sc, ctx, _ := getSpannerClient(t)
	defer sc.Close()

	tests := []struct {
		name               string
		params             map[string]interface{}
		jsonKey            string
		tsValue            time.Time
		rawValue           string
		shouldGetReflected bool
	}{
		{
			name: "Update with Key2",
			params: map[string]interface{}{
				"map_key": "Key2",
				"tsValue": time.Now(),
				"value1":  "user_idX",
			},
			jsonKey:            "Key2",
			tsValue:            time.Now(),
			rawValue:           "false",
			shouldGetReflected: true,
		},
		{
			name: "Update with Key3",
			params: map[string]interface{}{
				"map_key": "Key3",
				"tsValue": time.Now().Add(-time.Second * 2),
				"value1":  "user_idX",
			},
			jsonKey:            "Key3",
			tsValue:            time.Now().Add(-time.Second * 2),
			rawValue:           "true",
			shouldGetReflected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := responsehandler.QueryMetadata{
				SelectQueryMapUpdate: "SELECT test_ids FROM keyspace1_address_book_entries WHERE `user_id` = @value1 AND `last_commit_ts` <= @tsValue;",
				Query:                "UPDATE keyspace1_address_book_entries SET `last_commit_ts` = @tsValue, `test_ids` = @map_key WHERE `user_id` = @value1 AND `last_commit_ts` <= @tsValue;",
				TableName:            "address_book_entries",
				KeyspaceName:         KEYSPACE,
				ProtocalV:            4,
				QueryType:            QUERY_TYPE_UPDATE,
				Params:               tt.params,
				UpdateSetValues: []translator.UpdateSetValue{
					{
						Column:           "test_ids",
						Value:            "@map_key",
						RawValue:         tt.rawValue,
						IsMapUpdateByKey: true,
					},
				},
			}

			result, err := sc.UpdateMapByKey(ctx, query)
			assert.NoError(t, err, "Unexpected error in UpdateMapByKey")
			assert.Equal(t, 0, len(result.Data), "Result size should be 0 but found %d", len(result.Data))

			data, err := getDataFromSpanner(ctx, sc.Client, fmt.Sprintf("select * from keyspace1_address_book_entries WHERE `user_id`='%s';", tt.params["value1"]))
			if err != nil {
				t.Errorf(ErrorWhileFetchingFromSpanner, err)
			}
			if len(data) == 0 {
				t.Error(NotFoundOnSpanner)
			}
			if val, ok := data[0]["user_id"].(string); ok {
				assert.Equal(t, val, tt.params["value1"], fmt.Sprintf("expected %s but received %s", tt.params["value1"], val))
			} else {
				t.Errorf("unable to cast user_id %s", data[0]["user_id"])
			}
			val, err := castToBoolMap(data[0]["test_ids"])
			if err != nil {
				t.Error("error while casting to map[string]bool")
			} else {
				if len(val) != 2 {
					t.Errorf("Expected introduction of new key in  test_ids -  %v", val)
				}
				if val["Key2"] != false {
					t.Errorf("Invalid Update  for test_ids[key2] Expected false but got %v ", val["Key2"])
				}
			}
		})
	}
}

// castToBoolMap attempts to cast an interface{} to map[string]bool
func castToBoolMap(input interface{}) (map[string]bool, error) {
	// Attempt to cast directly to map[string]bool
	if output, ok := input.(map[string]bool); ok {
		return output, nil
	}

	// Attempt to cast to map[string]interface{} and convert to map[string]bool
	if inputMap, ok := input.(map[string]interface{}); ok {
		output := make(map[string]bool)
		for key, value := range inputMap {
			boolValue, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("value for key %s is not a bool", key)
			}
			output[key] = boolValue
		}
		return output, nil
	}

	return nil, fmt.Errorf("input is not of type map[string]bool or map[string]interface{}")
}

func TestFilterAndExecuteBatch(t *testing.T) {
	sc, ctx, _ := getSpannerClient(t)
	defer sc.Close()

	// Case 1: All insert queries with the same primary key
	t.Run("AllInsertWithSamePK", func(t *testing.T) {
		queries := make([]*responsehandler.QueryMetadata, 5)
		for i := 0; i < 5; i++ {
			tsValue := time.Now().Add(time.Second * time.Duration(-i*5))
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					TTL_TS:         time.Now().Add(time.Minute * 5),
					"user_id":      "same_pk_1",
					"upload_time":  5000000,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
					LAST_COMMIT_TS: &tsValue,
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"same_pk_1", 5000000, []byte(fmt.Sprintf("XYZ-%d", i)), &tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries[i] = query
		}

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf("Error while executing FilterAndExecuteBatch for all insert with same PK: %v", err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: AllInsertWithSamePK")
			assert.Equal(t, CommitAPI, batchQueryType)
		}

		data, err := getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_comm_upload_event WHERE `user_id`='same_pk_1' and `upload_time`= 5000000;")
		if err != nil {
			t.Errorf(ErrorWhileFetchingFromSpanner, err)
		} else if len(data) == 0 {
			t.Error(NotFoundOnSpanner)
		} else if val, ok := data[0]["comm_event"].([]byte); ok {
			assert.Equal(t, []byte("XYZ-0"), val, "Expected %s but received %s", []byte("XYZ-0"), val)
		} else {
			t.Errorf("Unable to cast comm_event %v", data[0]["comm_event"])
		}
	})

	// Case 2: All insert queries with different primary keys
	t.Run("AllInsertWithDifferentPK", func(t *testing.T) {
		queries := make([]*responsehandler.QueryMetadata, 5)
		for i := 0; i < 5; i++ {
			tsValue := time.Now()
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: tsValue,
					TTL_TS:         time.Now().Add(time.Second * 5),
					"user_id":      "diff_pk_1",
					"upload_time":  510000 + i,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"diff_pk_1", 510000 + i, []byte(fmt.Sprintf("XYZ-%d", i)), tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries[i] = query
		}

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf("Error while executing FilterAndExecuteBatch for all insert with different PK: %v", err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: AllInsertWithDifferentPK")
			assert.Equal(t, CommitAPI, batchQueryType)
		}

		data, err := getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_comm_upload_event WHERE `user_id`='diff_pk_1' order by upload_time;")
		if err != nil {
			t.Errorf(ErrorWhileFetchingFromSpanner, err)
		} else if len(data) == 0 {
			t.Error(NotFoundOnSpanner)
		} else if len(data) != 5 {
			t.Errorf("Expected 5 rows but received %d: %v", len(data), data)
		} else {
			for i, row := range data {
				if val, ok := row["upload_time"].(int64); ok {
					assert.Equal(t, 510000+int64(i), val, "Expected %d but received %d", 510000+int64(i), val)
				} else {
					t.Errorf("Unable to cast upload_time %v", row["upload_time"])
				}
			}
		}
	})

	// Case 3.1: Range delete
	t.Run("RangeDelete", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{
			{
				Query:        DELETE_COMM_UPLOAD_RANGE,
				QueryType:    QUERY_TYPE_DELETE,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Millisecond * -10), "value1": "diff_pk_1"},
				PrimaryKeys:  []string{"user_id", "upload_time"},
			},
		}

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf("Error while executing FilterAndExecuteBatch for range delete: %v", err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: RangeDelete")
			assert.Equal(t, ExecuteBatchDML, batchQueryType)
		}

		_, err = getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_comm_upload_event WHERE `user_id`='diff_pk_1' order by upload_time;")
		assert.Error(t, fmt.Errorf("no rows found"), err, "It should return no rows found")
	})

	// TODO : Revert this Unit test case post CI/CD pipeline fix causing failure for this test case
	// Case 3.2: Range delete followed by insert
	// t.Run("RangeDeleteFollowedByInsert", func(t *testing.T) {
	// 	queries := []*responsehandler.QueryMetadata{
	// 		{
	// 			Query:        DELETE_COMM_UPLOAD_RANGE,
	// 			QueryType:    QUERY_TYPE_DELETE,
	// 			TableName:    Table_COMM_UPLOAD,
	// 			KeyspaceName: KEYSPACE,
	// 			ProtocalV:    4,
	// 			Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Second * -10), "value1": "diff_pk_1"},
	// 			PrimaryKeys:  []string{"user_id", "upload_time"},
	// 			Paramkeys:    []string{"user_id", LAST_COMMIT_TS},
	// 			ParamValues:  []interface{}{"diff_pk_1", time.Now().Add(time.Second * -10)},
	// 		},
	// 	}

	// 	for i := 0; i < 5; i++ {
	// 		tsValue := time.Now().Add(time.Second * -2)
	// 		query := &responsehandler.QueryMetadata{
	// 			Query:        INSERT_COMM_UPLOAD,
	// 			TableName:    Table_COMM_UPLOAD,
	// 			KeyspaceName: KEYSPACE,
	// 			ProtocalV:    4,
	// 			QueryType:    "INSERT",
	// 			Params: map[string]interface{}{
	// 				LAST_COMMIT_TS: tsValue,
	// 				TTL_TS:         time.Now().Add(time.Minute * 5),
	// 				"user_id":      "diff_pk_1",
	// 				"upload_time":  510000 + i,
	// 				"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
	// 			},
	// 			Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
	// 			ParamValues:  []interface{}{"diff_pk_1", 510000 + i, []byte(fmt.Sprintf("XYZ-%d", i)), tsValue},
	// 			PrimaryKeys:  []string{"user_id", "upload_time"},
	// 			UsingTSCheck: UsingTsCheckQueryCommUpload,
	// 		}
	// 		queries = append(queries, query)
	// 	}

	// 	result, batchQueryType,  err := sc.FilterAndExecuteBatch(ctx, queries)
	// 	if err != nil {
	// 		t.Errorf(ErrorWhileFilterAndExecuteBatch, err)
	// 	} else {
	// 		assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: RangeDeleteFollowedByInsert")
	// 	}

	// 	data, err := getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_comm_upload_event WHERE `user_id`='diff_pk_1' order by upload_time;")
	// 	if err != nil {
	// 		t.Errorf(ErrorWhileFetchingFromSpanner, err)
	// 	} else if len(data) == 0 {
	// 		t.Errorf(NotFoundOnSpanner)
	// 	} else if len(data) != 5 {
	// 		t.Errorf("Expected 5 rows but received %d: %v", len(data), data)
	// 	} else {
	// 		for i, row := range data {
	// 			if val, ok := row["upload_time"].(int64); ok {
	// 				assert.Equal(t, 510000+int64(i), val, "Expected %d but received %d", 510000+int64(i), val)
	// 			} else {
	// 				t.Errorf("Unable to cast upload_time %v", row["upload_time"])
	// 			}
	// 		}
	// 	}
	// })

	// Case 4 -  Delete with full PK and multiple insert with same PK
	t.Run("PkBasedDeleteFollowedByInsertWithSamePK", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{
			{
				Query:        DELETE_COMM_UPLOAD_RANGE,
				QueryType:    QUERY_TYPE_DELETE,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Millisecond * -10), "value1": "same_pk_1", "value2": 5000000},
				PrimaryKeys:  []string{"user_id", "upload_time"},
			},
		}
		for i := 0; i < 5; i++ {
			tsValue := time.Now().Add(time.Second * time.Duration(-i*5))
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					TTL_TS:         time.Now().Add(time.Minute * 5),
					"user_id":      "same_pk_1",
					"upload_time":  5000000,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
					LAST_COMMIT_TS: &tsValue,
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"same_pk_1", 5000000, []byte(fmt.Sprintf("XYZ-%d", i)), &tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf("Error while executing FilterAndExecuteBatch for all insert with same PK: %v", err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: AllInsertWithSamePK")
			assert.Equal(t, ExecuteBatchDML, batchQueryType)
		}

		data, err := getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_comm_upload_event WHERE `user_id`='same_pk_1';")
		if err != nil {
			t.Errorf(ErrorWhileFetchingFromSpanner, err)
		} else if len(data) == 0 {
			t.Error(NotFoundOnSpanner)
		} else if val, ok := data[0]["comm_event"].([]byte); ok {
			assert.Equal(t, []byte("XYZ-0"), val, "Expected %s but received %s", []byte("XYZ-0"), val)
		} else {
			t.Errorf("Unable to cast comm_event %v", data[0]["comm_event"])
		}
	})
}

func TestFilterAndExecuteBatch2(t *testing.T) {
	// Case 5 -  Insert then range delete
	sc, ctx, _ := getSpannerClient(t)
	defer sc.Close()
	t.Run("MultipleInsertFollowedByRangeDelete", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}

		for i := 0; i < 5; i++ {
			tsValue := time.Now()
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: tsValue,
					TTL_TS:         time.Now().Add(time.Second * 5),
					"user_id":      "diff_pk_2",
					"upload_time":  510000 + i,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"diff_pk_2", 510000 + i, []byte(fmt.Sprintf("XYZ-%d", i)), tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_COMM_UPLOAD_RANGE,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    Table_COMM_UPLOAD,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Millisecond * -10), "value1": "diff_pk_2"},
			PrimaryKeys:  []string{"user_id", "upload_time"},
		})

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf(ErrorWhileFilterAndExecuteBatch, err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: MultipleInsertFollowedByRangeDelete")
			assert.Equal(t, ExecuteBatchDML, batchQueryType)
		}

		_, err = getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_comm_upload_event WHERE `user_id`='diff_pk_2' order by upload_time;")
		assert.Error(t, fmt.Errorf("no rows found"), err, "all rows should have deleted for MultipleInsertFollowedByRangeDelete")
	})

	// Case 6- part 1: Multiple insert with same PK with Followed by Delete
	t.Run("MultipleInsertWithSamePKFollowedByDelete", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}

		for i := 0; i < 5; i++ {
			tsValue := time.Now()
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					TTL_TS:         time.Now().Add(time.Second * 5),
					"user_id":      "same_pk_5",
					"upload_time":  510000,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"same_pk_5", 510000, []byte(fmt.Sprintf("XYZ-%d", i)), &tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_COMM_UPLOAD,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    Table_COMM_UPLOAD,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Millisecond * -10), "value1": "same_pk_5", "value2": 510000},
			PrimaryKeys:  []string{"user_id", "upload_time"},
		})

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf(ErrorWhileFilterAndExecuteBatch, err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: MultipleInsertWithSamePKFollowedByDelete")
			assert.Equal(t, ExecuteBatchDML, batchQueryType)
		}

		_, err = getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_comm_upload_event WHERE `user_id`='same_pk_5' order by upload_time;")
		assert.Error(t, fmt.Errorf("no rows found"), err, "all rows should have deleted for MultipleInsertWithSamePKFollowedByDelete")
	})
	// Case 6- part 2: Multiple insert with same PK [Error Case]
	t.Run("MultipleInsertWithSamePKFollowedByDeleteError", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}

		for i := 0; i < 2; i++ {
			tsValue := time.Now()
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: tsValue,
					TTL_TS:         time.Now().Add(time.Second * 5),
					"user_id":      "same_pk_5",
					"upload_time":  510000,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"same_pk_5", 510000, []byte(fmt.Sprintf("XYZ-%d", i)), tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}
		_, _, err := sc.FilterAndExecuteBatch(ctx, queries)
		assert.Error(t, fmt.Errorf("failed to filter queries: last_commit_ts is not a time.Time value"), err, "MultipleInsertWithSamePKFollowedByDeleteError should fail")
	})

	// Case 7 : Insert and range delete with same PKs
	t.Run("MultipleInsertFollowedByRangeDeleteOnSamePK", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}

		for i := 0; i < 5; i++ {
			tsValue := time.Now()
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					TTL_TS:         time.Now().Add(time.Second * 5),
					"user_id":      "same_pk_7",
					"upload_time":  510000,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"same_pk_7", 510000, []byte(fmt.Sprintf("XYZ-%d", i)), &tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_COMM_UPLOAD,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    Table_COMM_UPLOAD,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Millisecond * -10), "value1": "same_pk_7X", "value2": 510000},
			PrimaryKeys:  []string{"user_id", "upload_time"},
		})

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf(ErrorWhileFilterAndExecuteBatch, err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: MultipleInsertFollowedByRangeDeleteOnSamePK")
			assert.Equal(t, ExecuteBatchDML, batchQueryType)
		}

		_, err = getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_comm_upload_event WHERE `user_id`='same_pk_7X' order by upload_time;")
		assert.Error(t, fmt.Errorf("no rows found"), err, "all rows should have deleted for MultipleInsertFollowedByRangeDeleteOnSamePK")

		data, err := getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_comm_upload_event WHERE `user_id`='same_pk_7' order by upload_time;")
		if err != nil {
			t.Errorf(ErrorWhileFetchingFromSpanner, err)
		} else if len(data) != 1 {
			t.Errorf("Expected 1 rows but got %d", len(data))
		}
	})

	// Case 8 : Insert and range delete with different PKs
	t.Run("MultipleInsertFollowedByRangeDeleteOnDifferentPK", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}

		for i := 0; i < 5; i++ {
			tsValue := time.Now()
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					TTL_TS:         time.Now().Add(time.Second * 5),
					"user_id":      "diff_pk_7",
					"upload_time":  510000 + i,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"diff_pk_7", 510000 + i, []byte(fmt.Sprintf("XYZ-%d", i)), &tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_COMM_UPLOAD,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    Table_COMM_UPLOAD,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Millisecond * -10), "value1": "diff_pk_7X", "value2": 510000},
			PrimaryKeys:  []string{"user_id", "upload_time"},
		})

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf(ErrorWhileFilterAndExecuteBatch, err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: MultipleInsertFollowedByRangeDeleteOnDifferentPK")
			assert.Equal(t, ExecuteBatchDML, batchQueryType)
		}

		_, err = getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_comm_upload_event WHERE `user_id`='diff_pk_7X' order by upload_time;")
		assert.Error(t, fmt.Errorf("no rows found"), err, "all rows should have deleted for MultipleInsertFollowedByRangeDeleteOnDifferentPK")

		data, err := getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_comm_upload_event WHERE `user_id`='diff_pk_7' order by upload_time;")
		if err != nil {
			t.Errorf(ErrorWhileFetchingFromSpanner, err)
		} else if len(data) != 5 {
			t.Errorf("Expected 5 rows but got %d", len(data))
		}
	})

}

func TestFilterAndExecuteBatch3(t *testing.T) {
	sc, ctx, _ := getSpannerClient(t)
	defer sc.Close()
	// Case 9 : All operation with same PK multiple
	t.Run("AllOperationWithSamePKCase1", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}

		for i := 0; i < 5; i++ {
			tsValue := time.Now().Add(time.Second * -5)
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					TTL_TS:         time.Now().Add(time.Minute * 5),
					"user_id":      "same_pk_9",
					"upload_time":  510000,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"same_pk_9", 510000, []byte(fmt.Sprintf("XYZ-%d", i)), &tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_COMM_UPLOAD_RANGE,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    Table_COMM_UPLOAD,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Millisecond * -10), "value1": "same_pk_9"},
			PrimaryKeys:  []string{"user_id", "upload_time"},
		})
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_COMM_UPLOAD,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    Table_COMM_UPLOAD,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Millisecond * -10), "value1": "same_pk_9", "value2": 510000},
			PrimaryKeys:  []string{"user_id", "upload_time"},
		})

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf(ErrorWhileFilterAndExecuteBatch, err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: AllOperationWithSamePKCase1")
			assert.Equal(t, ExecuteBatchDML, batchQueryType)
		}
		_, err = getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_comm_upload_event WHERE `user_id`='same_pk_9' order by upload_time;")
		assert.Error(t, fmt.Errorf("no rows found"), err, "all rows should have deleted for AllOperationWithSamePKCase1")
	})
	// Case 10 - All Operation with map update by key
	t.Run("AllOperationWithSamePKUpdateMapByKey", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}

		for i := 0; i < 5; i++ {
			tsValue := time.Now().Add(time.Second * -5)
			query := &responsehandler.QueryMetadata{
				Query:        "INSERT INTO keyspace1_address_book_entries (user_id, test_ids, spanner_ttl_ts, last_commit_ts) values(@user_id, @test_ids ,@ttl_ts, @last_commit_ts );",
				TableName:    TABLE_ADDRESS,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					TTL_TS:         time.Now().Add(time.Minute * 5),
					"user_id":      "same_pk_10",
					"test_ids": spanner.NullJSON{
						Value: map[string]bool{
							fmt.Sprintf("key%d", i): true,
						},
						Valid: true,
					},
				},
				Paramkeys: []string{"user_id", "test_ids", TTL_TS, LAST_COMMIT_TS},
				ParamValues: []interface{}{"same_pk_10", spanner.NullJSON{
					Value: map[string]bool{
						fmt.Sprintf("key%d", i): true,
					},
					Valid: true,
				}, time.Now().Add(time.Minute * 5), &tsValue},
				PrimaryKeys:  []string{"user_id"},
				UsingTSCheck: "SELECT last_commit_ts FROM keyspace1_address_book_entries WHERE `user_id` = @user_id AND `last_commit_ts` > @last_commit_ts",
			}
			queries = append(queries, query)
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			SelectQueryMapUpdate: "SELECT test_ids FROM keyspace1_address_book_entries WHERE `user_id` = @value1 AND `last_commit_ts` <= @tsValue;",
			Query:                "UPDATE keyspace1_address_book_entries SET `last_commit_ts` = @tsValue, `test_ids` = @map_key WHERE `user_id` = @value1 AND `last_commit_ts` <= @tsValue;",
			TableName:            "address_book_entries",
			KeyspaceName:         KEYSPACE,
			ProtocalV:            4,
			QueryType:            QUERY_TYPE_UPDATE,
			Params: map[string]interface{}{
				LAST_COMMIT_TS: time.Now().Add(time.Second * -1),
				"tsValue":      time.Now().Add(time.Second * -1),
				TTL_TS:         time.Now().Add(time.Minute * 5),
				"value1":       "same_pk_10",
				"map_key":      "keyX",
			},
			UpdateSetValues: []translator.UpdateSetValue{
				{
					Column:           "test_ids",
					Value:            "@map_key",
					RawValue:         "false",
					IsMapUpdateByKey: true,
				},
			},
		})
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_ADDRESS_BOOK,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    TABLE_ADDRESS,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Millisecond * -10), "value1": "same_pk_10"},
			PrimaryKeys:  []string{"user_id"},
		})

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf(ErrorWhileFilterAndExecuteBatch, err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: AllOperationWithSamePKUpdateMapByKey")
			assert.Equal(t, BatchMixed, batchQueryType)
		}
		_, err = getDataFromSpanner(ctx, sc.Client, fmt.Sprintf("select * from %s WHERE `user_id`='same_pk_10';", TABLE_ADDRESS))
		assert.Error(t, fmt.Errorf("no rows found"), err, "all rows should have deleted for AllOperationWithSamePKUpdateMapByKey")
	})

	// case 11: delete followed by insert and update
	t.Run("AllOperationWithSamePKUpdateMapByKeyCase2", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_ADDRESS_BOOK,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    TABLE_ADDRESS,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Second * -10), "value1": "same_pk_11"},
			PrimaryKeys:  []string{"user_id"},
		})
		for i := 0; i < 5; i++ {
			tsValue := time.Now().Add(time.Second * -5)
			query := &responsehandler.QueryMetadata{
				Query:        "INSERT INTO keyspace1_address_book_entries (user_id, test_ids, spanner_ttl_ts, last_commit_ts) values(@user_id, @test_ids ,@ttl_ts, @last_commit_ts );",
				TableName:    TABLE_ADDRESS,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					TTL_TS:         time.Now().Add(time.Minute * 5),
					"user_id":      "same_pk_11",
					"test_ids": spanner.NullJSON{
						Value: map[string]bool{
							fmt.Sprintf("key%d", i): true,
						},
						Valid: true,
					},
				},
				Paramkeys: []string{"user_id", "test_ids", TTL_TS, LAST_COMMIT_TS},
				ParamValues: []interface{}{"same_pk_11", spanner.NullJSON{
					Value: map[string]bool{
						fmt.Sprintf("key%d", i): true,
					},
					Valid: true,
				}, time.Now().Add(time.Minute * 5), &tsValue},
				PrimaryKeys:  []string{"user_id"},
				UsingTSCheck: "SELECT last_commit_ts FROM keyspace1_address_book_entries WHERE `user_id` = @user_id AND `last_commit_ts` > @last_commit_ts",
			}
			queries = append(queries, query)
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			SelectQueryMapUpdate: "SELECT test_ids FROM keyspace1_address_book_entries WHERE `user_id` = @value1 AND `last_commit_ts` <= @tsValue;",
			Query:                "UPDATE keyspace1_address_book_entries SET `last_commit_ts` = @tsValue, `test_ids` = @map_key WHERE `user_id` = @value1 AND `last_commit_ts` <= @tsValue;",
			TableName:            "address_book_entries",
			KeyspaceName:         KEYSPACE,
			ProtocalV:            4,
			QueryType:            QUERY_TYPE_UPDATE,
			Params: map[string]interface{}{
				LAST_COMMIT_TS: time.Now().Add(time.Second * -1),
				"tsValue":      time.Now().Add(time.Second * -1),
				TTL_TS:         time.Now().Add(time.Minute * 5),
				"value1":       "same_pk_11",
				"map_key":      "keyX",
			},
			UpdateSetValues: []translator.UpdateSetValue{
				{
					Column:           "test_ids",
					Value:            "@map_key",
					RawValue:         "false",
					IsMapUpdateByKey: true,
				},
			},
		})

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf(ErrorWhileFilterAndExecuteBatch, err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: AllOperationWithSamePKUpdateMapByKeyCase2")
			assert.Equal(t, BatchMixed, batchQueryType)
		}
		data, err := getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_address_book_entries WHERE `user_id`='same_pk_11';")
		if err != nil {
			t.Errorf(ErrorWhileFetchingFromSpanner, err)
		} else if len(data) != 1 {
			t.Errorf("expected 1 row but received %d - %v", len(data), data)
		} else if mp, err := castToBoolMap(data[0]["test_ids"]); err == nil {
			for key, val := range mp {
				if key == "keyX" {
					assert.Equal(t, val, false, "expected value for key %s = false but got %v", key, val)
				} else if key == "key4" {
					assert.Equal(t, val, true, "expected value for key %s = false but got %v", key, val)
				} else {
					t.Errorf("Unexpected Key %s:%v", key, val)
				}
			}
		} else {
			t.Errorf("Unable to cast test_ids %v -%v", data[0]["test_ids"], err)
		}
	})
	// case 12 - complex and batch without using timestamp
	t.Run("AllOperationWithSamePKUpdateMapByKeyCase3", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        "DELETE FROM keyspace1_address_book_entries WHERE `user_id` = @value1",
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    TABLE_ADDRESS,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"value1": "same_pk_12"},
			PrimaryKeys:  []string{"user_id"},
		})
		for i := 0; i < 5; i++ {
			tsValue := time.Now().Add(time.Second * -5)
			query := &responsehandler.QueryMetadata{
				Query:        "INSERT INTO keyspace1_address_book_entries (user_id, test_ids, spanner_ttl_ts, last_commit_ts) values(@user_id, @test_ids ,@ttl_ts, @last_commit_ts );",
				TableName:    TABLE_ADDRESS,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					"user_id":      "same_pk_12",
					"test_ids": spanner.NullJSON{
						Value: map[string]bool{
							fmt.Sprintf("key%d", i): true,
						},
						Valid: true,
					},
				},
				Paramkeys: []string{"user_id", "test_ids", LAST_COMMIT_TS},
				ParamValues: []interface{}{"same_pk_12", spanner.NullJSON{
					Value: map[string]bool{
						fmt.Sprintf("key%d", i): true,
					},
					Valid: true,
				}, &tsValue},
				PrimaryKeys: []string{"user_id"},
			}
			queries = append(queries, query)
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			SelectQueryMapUpdate: "SELECT test_ids FROM keyspace1_address_book_entries WHERE `user_id` = @value1 AND `last_commit_ts` <= @tsValue;",
			Query:                "UPDATE keyspace1_address_book_entries SET `last_commit_ts` = @tsValue, `test_ids` = @map_key WHERE `user_id` = @value1",
			TableName:            "address_book_entries",
			KeyspaceName:         KEYSPACE,
			ProtocalV:            4,
			QueryType:            QUERY_TYPE_UPDATE,
			Params: map[string]interface{}{
				LAST_COMMIT_TS: time.Now().Add(time.Second * -1),
				"tsValue":      time.Now().Add(time.Second * -1),
				TTL_TS:         time.Now().Add(time.Minute * 5),
				"value1":       "same_pk_12",
				"map_key":      "keyX",
			},
			UpdateSetValues: []translator.UpdateSetValue{
				{
					Column:           "test_ids",
					Value:            "@map_key",
					RawValue:         "false",
					IsMapUpdateByKey: true,
				},
			},
		})

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf(ErrorWhileFilterAndExecuteBatch, err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: AllOperationWithSamePKUpdateMapByKeyCase3")
			assert.Equal(t, BatchMixed, batchQueryType)
		}
		data, err := getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_address_book_entries WHERE `user_id`='same_pk_12';")
		if err != nil {
			t.Errorf(ErrorWhileFetchingFromSpanner, err)
		} else if len(data) != 1 {
			t.Errorf("expected 1 row but received %d - %v", len(data), data)
		} else if mp, err := castToBoolMap(data[0]["test_ids"]); err == nil {
			for key, val := range mp {
				if key == "keyX" {
					assert.Equal(t, val, false, "expected value for key %s = false but got %v", key, val)
				} else if key == "key4" {
					assert.Equal(t, val, true, "expected value for key %s = false but got %v", key, val)
				} else {
					t.Errorf("Unexpected Key %s:%v", key, val)
				}
			}
		} else {
			t.Errorf("Unable to cast test_ids %v -%v", data[0]["test_ids"], err)
		}
	})

	//Case 13: Insert Followed by Update
	t.Run("InsertFollowedByUpdate", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}
		tsValue := time.Now().Add(time.Second * -5)
		query := &responsehandler.QueryMetadata{
			Query:        "INSERT INTO keyspace1_address_book_entries (user_id, test_ids, spanner_ttl_ts, last_commit_ts) values(@user_id, @test_ids ,@ttl_ts, @last_commit_ts );",
			TableName:    TABLE_ADDRESS,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			QueryType:    QUERY_TYPE_INSERT,
			Params: map[string]interface{}{
				LAST_COMMIT_TS: &tsValue,
				TTL_TS:         time.Now().Add(time.Minute * 5),
				"user_id":      "same_pk_13",
				"test_ids": spanner.NullJSON{
					Value: map[string]bool{
						"key": true,
					},
					Valid: true,
				},
			},
			Paramkeys: []string{"user_id", "test_ids", TTL_TS, LAST_COMMIT_TS},
			ParamValues: []interface{}{"same_pk_11", spanner.NullJSON{
				Value: map[string]bool{
					"key": true,
				},
				Valid: true,
			}, time.Now().Add(time.Minute * 5), &tsValue},
			PrimaryKeys:  []string{"user_id"},
			UsingTSCheck: "SELECT last_commit_ts FROM keyspace1_address_book_entries WHERE `user_id` = @user_id AND `last_commit_ts` > @last_commit_ts",
		}
		queries = append(queries, query)
		for i := 0; i < 5; i++ {
			queries = append(queries, &responsehandler.QueryMetadata{
				SelectQueryMapUpdate: "SELECT test_ids FROM keyspace1_address_book_entries WHERE `user_id` = @value1 AND `last_commit_ts` <= @tsValue;",
				Query:                "UPDATE keyspace1_address_book_entries SET `last_commit_ts` = @tsValue, `test_ids` = @map_key WHERE `user_id` = @value1 AND `last_commit_ts` <= @tsValue;",
				TableName:            "address_book_entries",
				KeyspaceName:         KEYSPACE,
				ProtocalV:            4,
				QueryType:            QUERY_TYPE_UPDATE,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: time.Now().Add(time.Second * -time.Duration(i)),
					"tsValue":      time.Now().Add(time.Second * -time.Duration(i)),
					TTL_TS:         time.Now().Add(time.Minute * 5),
					"value1":       "same_pk_13",
					"map_key":      fmt.Sprintf("key_%d", i),
				},
				UpdateSetValues: []translator.UpdateSetValue{
					{
						Column:           "test_ids",
						Value:            "@map_key",
						RawValue:         "false",
						IsMapUpdateByKey: true,
					},
				},
			})
		}

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf(ErrorWhileFilterAndExecuteBatch, err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: AllOperationWithSamePKUpdateMapByKeyCase2")
			assert.Equal(t, BatchMixed, batchQueryType)
		}
		data, err := getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_address_book_entries WHERE `user_id`='same_pk_13';")
		if err != nil {
			t.Errorf(ErrorWhileFetchingFromSpanner, err)
		} else if len(data) != 1 {
			t.Errorf("expected 1 row but received %d - %v", len(data), data)
		} else if mp, err := castToBoolMap(data[0]["test_ids"]); err == nil {
			for key, val := range mp {
				if strings.Contains(key, "key") {
					if key != "key" {
						assert.Equal(t, val, false, "expected value for key %s = false but got %v", key, val)
					} else {
						assert.Equal(t, val, true, "expected value for key %s = false but got %v", key, val)
					}
				} else {
					t.Errorf("Unexpected key val %v - %v", key, val)
				}
			}
		} else {
			t.Errorf("Unable to cast test_ids %v -%v", data[0]["test_ids"], err)
		}
	})

	//Case 13: All Update
	t.Run("AllUpdateByUpdate", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}
		for i := 0; i < 5; i++ {
			queries = append(queries, &responsehandler.QueryMetadata{
				SelectQueryMapUpdate: "SELECT test_ids FROM keyspace1_address_book_entries WHERE `user_id` = @value1 AND `last_commit_ts` <= @tsValue;",
				Query:                "UPDATE keyspace1_address_book_entries SET `last_commit_ts` = @tsValue, `test_ids` = @map_key WHERE `user_id` = @value1 AND `last_commit_ts` <= @tsValue;",
				TableName:            "address_book_entries",
				KeyspaceName:         KEYSPACE,
				ProtocalV:            4,
				QueryType:            QUERY_TYPE_UPDATE,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: time.Now().Add(time.Second * -time.Duration(i)),
					"tsValue":      time.Now().Add(time.Second * -time.Duration(i)),
					TTL_TS:         time.Now().Add(time.Minute * 5),
					"value1":       "same_pk_13",
					"map_key":      fmt.Sprintf("key_%d", i),
				},
				UpdateSetValues: []translator.UpdateSetValue{
					{
						Column:           "test_ids",
						Value:            "@map_key",
						RawValue:         "true",
						IsMapUpdateByKey: true,
					},
				},
			})
		}

		result, batchQueryType, err := sc.FilterAndExecuteBatch(ctx, queries)
		if err != nil {
			t.Errorf(ErrorWhileFilterAndExecuteBatch, err)
		} else {
			assert.Equal(t, 0, len(result.Data), "Unexpected number of rows in result for batch test case: AllOperationWithSamePKUpdateMapByKeyCase2")
			assert.Equal(t, BatchMixed, batchQueryType)
		}
		data, err := getDataFromSpanner(ctx, sc.Client, "select * from keyspace1_address_book_entries WHERE `user_id`='same_pk_13';")
		if err != nil {
			t.Errorf(ErrorWhileFetchingFromSpanner, err)
		} else if len(data) != 1 {
			t.Errorf("expected 1 row but received %d - %v", len(data), data)
		} else if mp, err := castToBoolMap(data[0]["test_ids"]); err == nil {
			for key, val := range mp {
				if strings.Contains(key, "key") {
					if key == "key_0" || key == "key" {
						assert.Equal(t, val, true, "expected value for key %s = false but got %v", key, val)
					} else {
						assert.Equal(t, val, false, "expected value for key %s = false but got %v", key, val)
					}
				} else {
					t.Errorf("Unexpected key val %v - %v", key, val)
				}
			}
		} else {
			t.Errorf("Unable to cast test_ids %v -%v", data[0]["test_ids"], err)
		}
	})

}

func TestFilterBatch(t *testing.T) {
	sc, ctx, _ := getSpannerClient(t)
	defer sc.Close()
	t.Run("MultipleInsertFollowedByRangeDeleteFilterBatch", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}
		for i := 0; i < 5; i++ {
			tsValue := time.Now().Add(time.Duration(time.Second * -1))
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					TTL_TS:         time.Now().Add(time.Second * 5),
					"user_id":      "diff_pk_2",
					"upload_time":  510000 + i,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"diff_pk_2", 510000 + i, []byte(fmt.Sprintf("XYZ-%d", i)), &tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_COMM_UPLOAD_RANGE,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    Table_COMM_UPLOAD,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Minute * -50), "value1": "diff_pk_2"},
			PrimaryKeys:  []string{"user_id", "upload_time"},
		})
		_, _ = sc.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			filteredResp, err := sc.filterBatch(ctx, txn, queries)
			if err != nil {
				t.Errorf(ErrorWhileFilterBatch, err)
			} else {

				assert.Equal(t, true, filteredResp.CanExecuteAsBatch, fmt.Sprintf("expected canExecuteAsBatch = true but got %v", filteredResp.CanExecuteAsBatch))
				assert.Equal(t, false, filteredResp.CanExecuteAsMutations, fmt.Sprintf("expected canExecuteAsMutations = true but got %v", filteredResp.CanExecuteAsMutations))
				assert.Equal(t, 1, len(filteredResp.Queries), fmt.Sprintf("expected 1 queries but got %d", len(filteredResp.Queries)))
			}
			return nil
		})
	})
	t.Run("MultipleInsertFollowedByRangeDeleteFilterBatchCase2", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}
		tsValue := time.Now()
		for i := 0; i < 5; i++ {
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					TTL_TS:         time.Now().Add(time.Second * 5),
					"user_id":      "diff_pk_2",
					"upload_time":  510000,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"diff_pk_2", 510000, []byte(fmt.Sprintf("XYZ-%d", i)), &tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_COMM_UPLOAD_RANGE,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    Table_COMM_UPLOAD,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"tsValue": time.Now().Add(time.Second * 50), "value1": "diff_pk_2"},
			PrimaryKeys:  []string{"user_id", "upload_time"},
		})
		_, _ = sc.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			filteredResp, err := sc.filterBatch(ctx, txn, queries)
			if err != nil {
				t.Errorf(ErrorWhileFilterBatch, err)
			} else {
				assert.Equal(t, true, filteredResp.CanExecuteAsBatch, fmt.Sprintf("MultipleInsertFollowedByRangeDeleteFilterBatchCase2 - expected canExecuteAsBatch = true but got %v", filteredResp.CanExecuteAsBatch))
				assert.Equal(t, false, filteredResp.CanExecuteAsMutations, fmt.Sprintf("expected canExecuteAsMutations = true but got %v", filteredResp.CanExecuteAsMutations))
				assert.Equal(t, 2, len(filteredResp.Queries), fmt.Sprintf("MultipleInsertFollowedByRangeDeleteFilterBatchCase2 - expected 6 queries but got %d", len(filteredResp.Queries)))
			}
			return nil
		})
	})

	t.Run("INSERT and DELETE via Mutations", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}
		tsValue := time.Now()
		for i := 0; i < 5; i++ {
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					TTL_TS:         time.Now().Add(time.Second * 5),
					"user_id":      "diff_pk_2",
					"upload_time":  510000,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"diff_pk_2", 510000, []byte(fmt.Sprintf("XYZ-%d", i)), &tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:            DELETE_COMM_UPLOAD_RANGE_WITHOUT_TS,
			QueryType:        QUERY_TYPE_DELETE,
			TableName:        Table_COMM_UPLOAD,
			KeyspaceName:     KEYSPACE,
			ProtocalV:        4,
			Params:           map[string]interface{}{"value1": "diff_pk_2"},
			PrimaryKeys:      []string{"user_id", "upload_time"},
			MutationKeyRange: []interface{}{"diff_pk_2"},
		})
		_, _ = sc.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			filteredResp, err := sc.filterBatch(ctx, txn, queries)
			if err != nil {
				t.Errorf(ErrorWhileFilterBatch, err)
			} else {
				assert.Equal(t, true, filteredResp.CanExecuteAsBatch, fmt.Sprintf("MultipleInsertFollowedByRangeDeleteFilterBatchCase2 - expected canExecuteAsBatch = true but got %v", filteredResp.CanExecuteAsBatch))
				assert.Equal(t, true, filteredResp.CanExecuteAsMutations, fmt.Sprintf("expected canExecuteAsMutations = true but got %v", filteredResp.CanExecuteAsMutations))
				assert.Equal(t, 2, len(filteredResp.Queries), fmt.Sprintf("MultipleInsertFollowedByRangeDeleteFilterBatchCase2 - expected 6 queries but got %d", len(filteredResp.Queries)))
			}
			return nil
		})
	})

	t.Run("INSERT and DELETE with batch update", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{}
		tsValue := time.Now()
		for i := 0; i < 5; i++ {
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					TTL_TS:         time.Now().Add(time.Second * 5),
					"user_id":      "diff_pk_2",
					"upload_time":  510000,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"diff_pk_2", 510000, []byte(fmt.Sprintf("XYZ-%d", i)), &tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_COMM_UPLOAD,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    Table_COMM_UPLOAD,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"value1": "diff_pk_2"},
			PrimaryKeys:  []string{"user_id", "upload_time"},
		})
		_, _ = sc.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			filteredResp, err := sc.filterBatch(ctx, txn, queries)
			if err != nil {
				t.Errorf(ErrorWhileFilterBatch, err)
			} else {
				assert.Equal(t, true, filteredResp.CanExecuteAsBatch, fmt.Sprintf("MultipleInsertFollowedByRangeDeleteFilterBatchCase2 - expected canExecuteAsBatch = true but got %v", filteredResp.CanExecuteAsBatch))
				assert.Equal(t, false, filteredResp.CanExecuteAsMutations, fmt.Sprintf("expected canExecuteAsMutations = true but got %v", filteredResp.CanExecuteAsMutations))
				assert.Equal(t, 2, len(filteredResp.Queries), fmt.Sprintf("MultipleInsertFollowedByRangeDeleteFilterBatchCase2 - expected 6 queries but got %d", len(filteredResp.Queries)))
			}
			return nil
		})
	})
}

func TestSortQueriesbyTS(t *testing.T) {
	// Define test cases
	tests := []struct {
		name     string
		input    []*responsehandler.QueryMetadata
		expected []*responsehandler.QueryMetadata
	}{
		{
			name:     "Empty input",
			input:    []*responsehandler.QueryMetadata{},
			expected: []*responsehandler.QueryMetadata{},
		},
		{
			name: "Single query",
			input: []*responsehandler.QueryMetadata{
				{QueryType: insertType, Params: map[string]interface{}{spannerTSColumn: time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)}},
			},
			expected: []*responsehandler.QueryMetadata{
				{QueryType: insertType, Params: map[string]interface{}{spannerTSColumn: time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)}},
			},
		},
		{
			name: "Multiple queries with insert and update types",
			input: []*responsehandler.QueryMetadata{
				{QueryType: QUERY_TYPE_UPDATE, Params: map[string]interface{}{tsColumnParam: time.Date(2023, 5, 3, 0, 0, 0, 0, time.UTC)}},
				{QueryType: insertType, Params: map[string]interface{}{spannerTSColumn: time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)}},
				{QueryType: QUERY_TYPE_UPDATE, Params: map[string]interface{}{tsColumnParam: time.Date(2023, 5, 2, 0, 0, 0, 0, time.UTC)}},
			},
			expected: []*responsehandler.QueryMetadata{
				{QueryType: insertType, Params: map[string]interface{}{spannerTSColumn: time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)}},
				{QueryType: QUERY_TYPE_UPDATE, Params: map[string]interface{}{tsColumnParam: time.Date(2023, 5, 2, 0, 0, 0, 0, time.UTC)}},
				{QueryType: QUERY_TYPE_UPDATE, Params: map[string]interface{}{tsColumnParam: time.Date(2023, 5, 3, 0, 0, 0, 0, time.UTC)}},
			},
		},
		{
			name: "Queries with missing timestamps",
			input: []*responsehandler.QueryMetadata{
				{QueryType: QUERY_TYPE_UPDATE, Params: map[string]interface{}{tsColumnParam: time.Date(2023, 5, 3, 0, 0, 0, 0, time.UTC)}},
				{QueryType: insertType, Params: map[string]interface{}{}},
				{QueryType: QUERY_TYPE_UPDATE, Params: map[string]interface{}{tsColumnParam: time.Date(2023, 5, 2, 0, 0, 0, 0, time.UTC)}},
			},
			expected: []*responsehandler.QueryMetadata{
				{QueryType: QUERY_TYPE_UPDATE, Params: map[string]interface{}{tsColumnParam: time.Date(2023, 5, 2, 0, 0, 0, 0, time.UTC)}},
				{QueryType: QUERY_TYPE_UPDATE, Params: map[string]interface{}{tsColumnParam: time.Date(2023, 5, 3, 0, 0, 0, 0, time.UTC)}},
				{QueryType: insertType, Params: map[string]interface{}{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sortQueriesbyTS(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Expected: %v, got: %v", tt.expected, result)
			}
		})
	}
}

func TestFilterByTimestamp(t *testing.T) {
	t.Run("MultipleInsertFollowedByRangeDeleteFilterByTimestamp", func(t *testing.T) {
		var largestTime = time.Now().Add(time.Hour * -1)
		queries := []*responsehandler.QueryMetadata{}
		for i := 0; i < 5; i++ {
			tsValue := time.Now().Add(time.Duration(time.Second * time.Duration(i) * -1))
			if largestTime.Before(tsValue) {
				largestTime = tsValue
			}
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					"user_id":      "diff_pk_2",
					"upload_time":  510000 + i,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"diff_pk_2", 510000 + i, []byte(fmt.Sprintf("XYZ-%d", i)), &tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}
		tsValue := time.Now().Add(time.Second * -50)
		if largestTime.Before(tsValue) {
			largestTime = tsValue
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_COMM_UPLOAD_RANGE,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    Table_COMM_UPLOAD,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{LAST_COMMIT_TS: &tsValue, "value1": "diff_pk_2"},
			PrimaryKeys:  []string{"user_id", "upload_time"},
		})

		query, err := filterByTimestamp(queries)
		if err != nil {
			t.Fatalf("error while executing - filterByTimestamp - %v", err)
		}

		if query.Params[LAST_COMMIT_TS] != nil {
			if !largestTime.Equal(*(query.Params[LAST_COMMIT_TS].(*time.Time))) {
				t.Error(largestTimeStampMismatchError)
			}
		} else if query.Params["tsValue"] != nil {
			if !largestTime.Equal(query.Params["tsValue"].(time.Time)) {
				t.Error(largestTimeStampMismatchError)
			}
		}
	})

	t.Run("SomeQueriesWithoutTimestamp", func(t *testing.T) {
		var largestTime = time.Now().Add(time.Hour * -1)
		queries := []*responsehandler.QueryMetadata{}
		for i := 0; i < 5; i++ {
			tsValue := time.Now().Add(time.Duration(time.Second * time.Duration(i) * -1))
			if largestTime.Before(tsValue) {
				largestTime = tsValue
			}
			query := &responsehandler.QueryMetadata{
				Query:        INSERT_COMM_UPLOAD,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				QueryType:    QUERY_TYPE_INSERT,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
					"user_id":      "diff_pk_2",
					"upload_time":  510000 + i,
					"comm_event":   []byte(fmt.Sprintf("XYZ-%d", i)),
				},
				Paramkeys:    []string{"user_id", "upload_time", "comm_event", LAST_COMMIT_TS},
				ParamValues:  []interface{}{"diff_pk_2", 510000 + i, []byte(fmt.Sprintf("XYZ-%d", i)), &tsValue},
				PrimaryKeys:  []string{"user_id", "upload_time"},
				UsingTSCheck: UsingTsCheckQueryCommUpload,
			}
			queries = append(queries, query)
		}
		tsValue := time.Now().Add(time.Second * -50)
		if largestTime.Before(tsValue) {
			largestTime = tsValue
		}
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_COMM_UPLOAD_RANGE,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    Table_COMM_UPLOAD,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{LAST_COMMIT_TS: &tsValue, "value1": "diff_pk_2"},
			PrimaryKeys:  []string{"user_id", "upload_time"},
		})
		// Add a query without a timestamp
		queries = append(queries, &responsehandler.QueryMetadata{
			Query:        DELETE_COMM_UPLOAD_RANGE,
			QueryType:    QUERY_TYPE_DELETE,
			TableName:    Table_COMM_UPLOAD,
			KeyspaceName: KEYSPACE,
			ProtocalV:    4,
			Params:       map[string]interface{}{"value1": "diff_pk_2"},
			PrimaryKeys:  []string{"user_id", "upload_time"},
		})

		query, err := filterByTimestamp(queries)
		if err != nil {
			t.Fatalf("Error while executing - filterByTimestamp - %v", err)
		}

		if query.Params[LAST_COMMIT_TS] != nil {
			if !largestTime.Equal(*(query.Params[LAST_COMMIT_TS].(*time.Time))) {
				t.Error(largestTimeStampMismatchError)
			}
		} else if query.Params["tsValue"] != nil {
			if !largestTime.Equal(query.Params["tsValue"].(time.Time)) {
				t.Error(largestTimeStampMismatchError)
			}
		}
	})

	t.Run("InvalidTimestampType", func(t *testing.T) {
		queries := []*responsehandler.QueryMetadata{
			{
				Query:        INSERT_COMM_UPLOAD,
				QueryType:    QUERY_TYPE_INSERT,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: "invalid type",
				},
				PrimaryKeys: []string{"user_id", "upload_time"},
			},
		}

		_, err := filterByTimestamp(queries)
		if err == nil {
			t.Fatalf("Expected error due to invalid timestamp type, but got none")
		}
	})

	t.Run("EqualTimestamps", func(t *testing.T) {
		tsValue := time.Now()
		queries := []*responsehandler.QueryMetadata{
			{
				Query:        INSERT_COMM_UPLOAD,
				QueryType:    QUERY_TYPE_INSERT,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
				},
				PrimaryKeys: []string{"user_id", "upload_time"},
			},
			{
				Query:        INSERT_COMM_UPLOAD,
				QueryType:    QUERY_TYPE_INSERT,
				TableName:    Table_COMM_UPLOAD,
				KeyspaceName: KEYSPACE,
				ProtocalV:    4,
				Params: map[string]interface{}{
					LAST_COMMIT_TS: &tsValue,
				},
				PrimaryKeys: []string{"user_id", "upload_time"},
			},
		}

		query, err := filterByTimestamp(queries)
		if err != nil {
			t.Fatalf("Error while executing - filterByTimestamp - %v", err)
		}

		if query.Params[LAST_COMMIT_TS] != nil {
			if !tsValue.Equal(*(query.Params[LAST_COMMIT_TS].(*time.Time))) {
				t.Error("Equal timestamps are not matching")
			}
		} else {
			t.Error("No valid timestamp found")
		}
	})
}

func TestBuildResponse(t *testing.T) {
	t.Run("BasicScenario", func(t *testing.T) {
		now := time.Now()
		earlier := now.Add(-time.Hour)
		later := now.Add(time.Hour)

		pkData := map[string]*responsehandler.QueryMetadata{
			"pk1": {Params: map[string]interface{}{spannerTSColumn: &earlier}},
			"pk2": {Params: map[string]interface{}{spannerTSColumn: &later}},
		}

		filteredResponse := map[string]interface{}{
			"pk1": &now,
			"pk2": &earlier,
		}

		response, err := buildResponse(pkData, filteredResponse)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(response) != 1 {
			t.Fatalf("expected 1 entry in response, got %d", len(response))
		}

		if response["pk2"].Params[spannerTSColumn] != pkData["pk2"].Params[spannerTSColumn] {
			t.Errorf("expected pk2 to be updated")
		}
	})

	t.Run("MissingTimestampInFilteredResponse", func(t *testing.T) {
		now := time.Now()

		pkData := map[string]*responsehandler.QueryMetadata{
			"pk1": {Params: map[string]interface{}{spannerTSColumn: &now}},
		}

		filteredResponse := map[string]interface{}{
			"pk1": "invalid type",
		}

		_, err := buildResponse(pkData, filteredResponse)
		if err == nil {
			t.Fatalf("expected error, got none")
		}

		expectedError := "expected last_commit_ts to be a time.Time value, got string"
		if err.Error() != expectedError {
			t.Errorf("expected error message '%s', got '%s'", expectedError, err.Error())
		}
	})

	t.Run("MissingTimestampInPKData", func(t *testing.T) {
		now := time.Now()

		pkData := map[string]*responsehandler.QueryMetadata{
			"pk1": {Params: map[string]interface{}{spannerTSColumn: "invalid type"}},
		}

		filteredResponse := map[string]interface{}{
			"pk1": &now,
		}

		_, err := buildResponse(pkData, filteredResponse)
		if err == nil {
			t.Fatalf("expected error, got none")
		}

		expectedError := "expected query timestamp to be a time.Time value, got string"
		if err.Error() != expectedError {
			t.Errorf("expected error message '%s', got '%s'", expectedError, err.Error())
		}
	})

	t.Run("EmptyFilteredResponse", func(t *testing.T) {
		now := time.Now()

		pkData := map[string]*responsehandler.QueryMetadata{
			"pk1": {Params: map[string]interface{}{spannerTSColumn: &now}},
		}

		filteredResponse := map[string]interface{}{}

		response, err := buildResponse(pkData, filteredResponse)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(response) != 1 {
			t.Fatalf("expected 1 entry in response, got %d", len(response))
		}

		if response["pk1"].Params[spannerTSColumn] != pkData["pk1"].Params[spannerTSColumn] {
			t.Errorf("expected pk1 to be included in response")
		}
	})

	t.Run("EmptyPKData", func(t *testing.T) {
		filteredResponse := map[string]interface{}{
			"pk1": time.Now(),
		}

		response, err := buildResponse(map[string]*responsehandler.QueryMetadata{}, filteredResponse)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(response) != 0 {
			t.Fatalf("expected 0 entries in response, got %d", len(response))
		}
	})

	t.Run("EqualTimestamps", func(t *testing.T) {
		tsValue := time.Now()
		queries := []*responsehandler.QueryMetadata{
			{
				Params: map[string]interface{}{spannerTSColumn: &tsValue},
			},
			{
				Params: map[string]interface{}{spannerTSColumn: &tsValue},
			},
		}

		response, err := buildResponse(map[string]*responsehandler.QueryMetadata{
			"pk1": queries[0],
			"pk2": queries[1],
		}, map[string]interface{}{
			"pk1": &tsValue,
			"pk2": &tsValue,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(response) != 0 {
			t.Fatalf("expected 0 entries in response, got %d", len(response))
		}
	})
}

func TestSingleReadForBatch(t *testing.T) {
	sc, ctx, err := getSpannerClient(t)
	if err != nil {
		t.Fatalf("Failed to get Spanner client: %v", err)
	}
	defer sc.Close()

	// Insert initial data into Spanner
	err = insertTestData(ctx, sc.Client)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}
	tm := time.Now()
	tests := []struct {
		name            string
		pkData          map[string]*responsehandler.QueryMetadata
		tableName       string
		expectedLen     int
		expectedErr     error
		expectedResults map[string]*responsehandler.QueryMetadata
	}{
		{
			name: "Basic scenario",
			pkData: map[string]*responsehandler.QueryMetadata{
				"pk1": {Params: map[string]interface{}{"user_id": "user1", "upload_time": int64(1000), spannerTSColumn: tm.Add(-time.Hour)}, PrimaryKeys: []string{"user_id", "upload_time"}},
				"pk2": {Params: map[string]interface{}{"user_id": "user2", "upload_time": int64(2000), spannerTSColumn: tm}, PrimaryKeys: []string{"user_id", "upload_time"}},
			},
			tableName:   "keyspace1_comm_upload_event",
			expectedLen: 2,
			expectedErr: nil,
			expectedResults: map[string]*responsehandler.QueryMetadata{
				"pk1": {Params: map[string]interface{}{"user_id": "user1", "upload_time": int64(1000), spannerTSColumn: tm.Add(-time.Hour)}, PrimaryKeys: []string{"user_id", "upload_time"}},
				"pk2": {Params: map[string]interface{}{"user_id": "user2", "upload_time": int64(2000), spannerTSColumn: tm}, PrimaryKeys: []string{"user_id", "upload_time"}},
			},
		},
		{
			name:        "Empty primary key data",
			pkData:      map[string]*responsehandler.QueryMetadata{},
			tableName:   "keyspace1_comm_upload_event",
			expectedLen: 0,
			expectedErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _ = sc.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				result, err := sc.singleReadForBatch(ctx, txn, tt.pkData, tt.tableName)
				if tt.expectedErr != nil {
					assert.EqualError(t, err, tt.expectedErr.Error())
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, tt.expectedLen, len(result))
				if tt.expectedResults != nil {
					for k, v := range tt.expectedResults {
						assert.Equal(t, v, result[k])
					}
				}
				return nil
			})
		})
	}
}

func insertTestData(ctx context.Context, client *spanner.Client) error {
	// Insert initial data into the test table
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("keyspace1_comm_upload_event", []string{"user_id", "upload_time", "comm_event", "spanner_ttl_ts", "last_commit_ts"},
			[]interface{}{"user1", int64(1000), []byte("event1"), spanner.NullTime{}, time.Now().Add(-time.Hour)}),
		spanner.InsertOrUpdate("keyspace1_comm_upload_event", []string{"user_id", "upload_time", "comm_event", "spanner_ttl_ts", "last_commit_ts"},
			[]interface{}{"user2", int64(2000), []byte("event2"), spanner.NullTime{}, time.Now()}),
	})
	return err
}
