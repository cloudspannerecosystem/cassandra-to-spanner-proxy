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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/docker/docker/api/types/container"
	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	secretPath          = os.Getenv("INTEGRATION_TEST_CRED_PATH")
	credentialsFilePath = "/tmp/keys/service-account.json"
	credentialsPath     = "/tmp/keys/"
	containerImage      = "asia-south1-docker.pkg.dev/cassandra-to-spanner/spanner-adaptor-docker/spanner-adaptor:unit-test"
)

var (
	isProxy = false
)

const (
	keyspace                 = "keyspace1"
	table                    = "event"
	selectQuery              = `SELECT event_data, upload_time, user_id FROM keyspace1.event WHERE upload_time = ? AND user_id = ?`
	rangeSelect              = `SELECT event_data, upload_time, user_id FROM keyspace1.event WHERE user_id = ?;`
	insertQuery              = `INSERT INTO keyspace1.event (event_data, upload_time, user_id) VALUES (?, ?, ?)`
	updateQuery              = `update keyspace1.event set event_data = ? where upload_time = ? and user_id = ? ;`
	updateQueryWithTTL       = `update keyspace1.event using TTL ? set event_data = ? where upload_time = ? and user_id = ? ;`
	updateQueryWithTTLAndTS  = `update keyspace1.event using TTL ? and timestamp ? set event_data = ? where upload_time = ? and user_id = ? ;`
	partialInsertQuery       = `INSERT INTO keyspace1.event ( upload_time, user_id) VALUES ( ?, ?)`
	insertQueryWithTimestamp = `INSERT INTO keyspace1.event (event_data, upload_time, user_id) VALUES (?, ?, ?) USING TIMESTAMP ?`
	insertQueryWithTTL       = `INSERT INTO keyspace1.event (event_data, upload_time, user_id) VALUES (?, ?, ?) USING TTL ?`
	insertQueryWithTTLAndTS  = `INSERT INTO keyspace1.event (event_data, upload_time, user_id) VALUES (?, ?, ?) USING TIMESTAMP ? and TTL ?`
	rangeDeleteWithTS        = "DELETE FROM keyspace1.event USING TIMESTAMP ? WHERE user_id = ?"
	deleteWithTS             = "DELETE FROM keyspace1.event USING TIMESTAMP ? WHERE upload_time = ? AND user_id = ?"
	basicSelectWithLimit     = "SELECT * FROM keyspace1.event limit ?"
	selectColWithOrderBy     = "SELECT upload_time, event_data FROM keyspace1.event WHERE user_id = ? ORDER BY upload_time ASC;"
)
const (
	setupFailedError          = "setup failed: %v"
	errorWhileDelete          = "Error while deleting - %s"
	expectedZeroRows          = "expected 0 rows post delete operation"
	failedToUpdateRow         = "failed to update row: %v"
	errorWhileInsert          = "error occurred while insert"
	errorWhileInsertTS        = "%s with timestamp & ttl - %s"
	errorInSelect             = "error in selecting: %v"
	errorMoreRowsThanExpected = "Received more rows than expected"
	unexpectedRow             = "Unexpected row %d: got (%d, %s), expected (%d, %s)"
	errorInClosingIter        = "error closing iterator: %v"
	errorExpectedRows         = "Expected %d rows, but got %d"
	errorValidationFailed     = "Validation failed for %s: expected %v, got %v"
	errorValidationType       = "Validation failed for %s: expected %s, got %T"
	errorQueryFailed          = "Query failed: %v"
	errorInsertFailed         = "Insert failed: %v"
	errorWhileExecutingBatch  = "Error while executing Batch - %s"
)

const (
	typeTime            = "time.Time"
	typeListOfByte      = "[]byte"
	typeMapStringBool   = "map[string]bool"
	typeMapStringString = "map[string]string"
	typeMapStringTime   = "map[string]time.Time"
	typeListOfString    = "[]string"
)

const (
	insertRow = "insert Row"
	updateRow = "Updated Row"

	updateInFutureError = "Expected failure as updating in future"
)

var (
	session *gocql.Session
)

func fetchAndStoreCredentials(ctx context.Context) error {
	if secretPath == "" {
		return fmt.Errorf("ENV INTEGRATION_TEST_CRED_PATH IS NOT SET")
	}
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create secret manager client: %v", err)
	}
	defer client.Close()

	accessRequest := &secretmanagerpb.AccessSecretVersionRequest{
		Name: secretPath,
	}

	result, err := client.AccessSecretVersion(ctx, accessRequest)
	if err != nil {
		return fmt.Errorf("failed to access secret version: %v", err)
	}

	if err := os.MkdirAll(credentialsPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	credentials := string(result.Payload.Data)
	err = os.WriteFile(credentialsFilePath, []byte(credentials), 0644)
	if err != nil {
		return fmt.Errorf("failed to write credentials to file: %v", err)
	}

	return nil
}

func TestMain(m *testing.M) {
	var target string
	flag.StringVar(&target, "target", "proxy", "Specify the test target: 'proxy' or 'cassandra'")
	flag.Parse()

	if target == "" {
		target = "proxy"
	}
	switch target {
	case "proxy":
		isProxy = true
		setupAndRunSpannerProxy(m)
	case "cassandra":
		isProxy = false
		setupAndRunCassandra(m)
	default:
		log.Fatalf("Invalid target - %s", target)
	}
}

func setupAndRunSpannerProxy(m *testing.M) {
	ctx := context.Background()
	if err := fetchAndStoreCredentials(ctx); err != nil {
		log.Fatalf("error while setting up gcp credentials - %v", err)
	}

	//TODO Get Spanner connection and create/update DDL and tableConfig

	// Request a testcontainers.Container object
	req := testcontainers.ContainerRequest{
		Image:        containerImage,
		ExposedPorts: []string{"9042/tcp"},
		Env: map[string]string{
			"GOOGLE_APPLICATION_CREDENTIALS": "/tmp/keys/service-account.json",
		},
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			hostConfig.Binds = []string{
				fmt.Sprintf("%s:/tmp/keys/service-account.json", credentialsFilePath),
			}
		},
		WaitingFor: wait.ForLog("proxy is listening").WithStartupTimeout(60 * time.Second),
	}

	proxyContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("Could not start container: %v", err)
	}
	defer func() {
		err := proxyContainer.Terminate(ctx)
		log.Fatalf("error while terminating - %v", err.Error())
	}()

	host, err := proxyContainer.Host(ctx)
	if err != nil {
		log.Fatalf("Could not get container host: %v", err)
	}

	mappedPort, err := proxyContainer.MappedPort(ctx, "9042")
	if err != nil {
		log.Fatalf("Could not get mapped port: %v", err)
	}

	cluster := gocql.NewCluster(host)
	cluster.Port = mappedPort.Int()
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = 4

	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatalf("Could not connect to Cassandra: %v", err)
	}
	defer session.Close()

	// Run tests
	code := m.Run()

	// Cleanup
	session.Close()
	os.Exit(code)
}

func setupAndRunCassandra(m *testing.M) {
	// Create a context with a 5-minute timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Fetch and store GCP credentials if needed
	if err := fetchAndStoreCredentials(ctx); err != nil {
		log.Fatalf("Error while setting up GCP credentials - %v", err)
	}

	// Define the container request
	req := testcontainers.ContainerRequest{
		Image:        "cassandra:latest",
		ExposedPorts: []string{"9042/tcp"}, // Expose the default Cassandra CQL port
		Env: map[string]string{
			"MAX_HEAP_SIZE": "512M", // Optional tuning for Cassandra
			"HEAP_NEWSIZE":  "100M",
		},
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			// Optional: Mount GCP credentials if needed
			hostConfig.Binds = []string{
				fmt.Sprintf("%s:/tmp/keys/service-account.json", credentialsFilePath),
			}
		},
		WaitingFor: wait.ForLog("Starting listening for CQL clients on /0.0.0.0:9042").WithStartupTimeout(120 * time.Second), // Wait until Cassandra is ready
	}

	// Start the Cassandra container
	cassandraContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("Could not start container: %v", err)
	}
	defer func() {
		// Ensure the container is terminated after tests
		if err := cassandraContainer.Terminate(ctx); err != nil {
			log.Fatalf("Error while terminating container - %v", err)
		}
	}()

	// Get the container host and mapped port for Cassandra
	host, err := cassandraContainer.Host(ctx)
	if err != nil {
		log.Fatalf("Could not get container host: %v", err)
	}

	mappedPort, err := cassandraContainer.MappedPort(ctx, "9042")
	if err != nil {
		log.Fatalf("Could not get mapped port: %v", err)
	}

	// Configure and create a new Cassandra cluster session
	cluster := gocql.NewCluster(host)
	cluster.Port = mappedPort.Int()           // Use the mapped port from the container
	cluster.ProtoVersion = 4                  // Use protocol version 4
	cluster.ConnectTimeout = 30 * time.Second // Set connection timeout
	cluster.Keyspace = "system"               // Use the default 'system' keyspace initially

	// Create a session with Cassandra
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatalf("Could not connect to Cassandra: %v", err)
	}
	defer session.Close()

	//create Kespace
	ddlStatementKeyspace := "CREATE KEYSPACE IF NOT EXISTS keyspace1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };"
	if err := session.Query(ddlStatementKeyspace).Exec(); err != nil {
		log.Fatalf("Could not create keyspace: %v", err)
	}

	ddlStatement := "CREATE TABLE IF NOT EXISTS keyspace1.event ( user_id text, upload_time bigint, event_data blob, PRIMARY KEY (user_id, upload_time)) WITH bloom_filter_fp_chance = 0.01;"
	if err := session.Query(ddlStatement).Exec(); err != nil {
		log.Fatalf("Could not create table: %v", err)
	}

	ddlStatement = "CREATE TABLE IF NOT EXISTS keyspace1.validate_data_types (text_col text, blob_col blob, timestamp_col TIMESTAMP, int_col int, bigint_col bigint, float_col float, bool_col boolean, uuid_col uuid, map_bool_col map<text, boolean>, map_str_col map<text, text>, map_ts_col map<text, timestamp>, list_str_col list<text>, set_str_col set<text>, PRIMARY KEY (text_col) ) WITH bloom_filter_fp_chance=0.010000;"
	if err := session.Query(ddlStatement).Exec(); err != nil {
		log.Fatalf("Could not create table: %v", err)
	}

	// Run the tests
	code := m.Run()

	// Cleanup and exit
	session.Close()
	os.Exit(code)
}

func insertData(t *testing.T, session *gocql.Session, commEvent []byte, uploadTime int64, userID string) {
	if err := session.Query(insertQuery, commEvent, uploadTime, userID).Exec(); err != nil {
		t.Fatalf("failed to insert data: %v, %v , %v,  %v", userID, uploadTime, string(commEvent), err)
	}
}

func partialInsertData(t *testing.T, session *gocql.Session, uploadTime int64, userID string) {
	if err := session.Query(partialInsertQuery, uploadTime, userID).Exec(); err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}
}

func selectAndValidateData(t *testing.T, session *gocql.Session, expectedCommEvent []byte, expectedUploadTime int64, expectedUserID string) {
	var retrievedCommEvent []byte
	var retrievedUploadTime int64
	var retrievedUserID string

	if err := session.Query(selectQuery, expectedUploadTime, expectedUserID).Scan(&retrievedCommEvent, &retrievedUploadTime, &retrievedUserID); err != nil {
		t.Fatalf("failed to select data: %v", err)
	}
	if !cmp.Equal(string(retrievedCommEvent), string(expectedCommEvent)) {
		t.Errorf("CommEvent diff: %s", cmp.Diff(string(retrievedCommEvent), string(expectedCommEvent)))
	}

	if !cmp.Equal(retrievedUploadTime, expectedUploadTime) {
		t.Errorf("UploadTime diff: %s", cmp.Diff(retrievedUploadTime, expectedUploadTime))
	}

	if !cmp.Equal(retrievedUserID, expectedUserID) {
		t.Errorf("UserID diff: %s", cmp.Diff(retrievedUserID, expectedUserID))
	}
}

func updateData(t *testing.T, session *gocql.Session, commEvent []byte, uploadTime int64, userID string) {
	if err := session.Query(updateQuery, commEvent, uploadTime, userID).Exec(); err != nil {
		t.Fatalf(failedToUpdateRow, err)
	}
}

func TestIntegration_Insert(t *testing.T) {
	tests := []struct {
		name        string
		runTest     func(t *testing.T)
		expectedErr error
	}{
		{
			name: "Basic Insert ",
			runTest: func(t *testing.T) {
				commEvent := []byte("event data")
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				insertData(t, session, commEvent, uploadTime, userID)
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "Upsert",
			runTest: func(t *testing.T) {
				commEvent := []byte(updateRow)
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				insertData(t, session, []byte(insertRow), uploadTime, userID)
				insertData(t, session, commEvent, uploadTime, userID) //upsert
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "Insert with Future Timestamp",
			runTest: func(t *testing.T) {
				commEvent := []byte("future event data")
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				futureTimestamp := time.Now().Add(time.Hour).UnixNano() / int64(time.Microsecond)
				err := session.Query(insertQueryWithTimestamp, commEvent, uploadTime, userID, futureTimestamp).Exec()
				if isProxy {
					if err == nil {
						t.Fatalf("expected error on insert with future timestamp, but insert succeeded")
					} else {
						expectedError := "Cannot write timestamps in the future"
						if strings.Contains(err.Error(), expectedError) {
							t.Logf("Insert with future timestamp failed as expected: %v", err)
						} else {
							t.Fatalf("unexpected error message: %v", err)
						}
					}
				} else if err != nil {
					t.Errorf("error not expected - %s", err)
				}
			},
			expectedErr: nil,
		},
		{
			name: "Insert with past Timestamp",
			runTest: func(t *testing.T) {
				commEvent := []byte("past event data")
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				insertData(t, session, []byte(insertRow), uploadTime, userID)
				pastTimestamp := time.Now().Add(-1*time.Hour).UnixNano() / int64(time.Microsecond)

				err := session.Query(insertQueryWithTimestamp, commEvent, uploadTime, userID, pastTimestamp).Exec()
				if err != nil {
					t.Fatalf("Upsert with past timestamp failed")
				}

				var retrievedCommEvent []byte
				var retrievedUploadTime int64
				var retrievedUserID string

				if err := session.Query(selectQuery, uploadTime, userID).Scan(&retrievedCommEvent, &retrievedUploadTime, &retrievedUserID); err != nil {
					t.Fatalf("failed to select data: %v", err)
				}

				if string(retrievedCommEvent) == string(commEvent) {
					t.Error("upsert should be ignored")
				}
			},
			expectedErr: nil,
		},
		{
			name: "Insert with TTL",
			runTest: func(t *testing.T) {
				commEvent := []byte("event with TTL")
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				ttl := 10 // 10 seconds

				err := session.Query(insertQueryWithTTL, commEvent, uploadTime, userID, ttl).Exec()
				if err != nil {
					t.Fatalf("%s with ttl - %s", errorWhileInsert, err.Error())
				}
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "Upsert with TTL",
			runTest: func(t *testing.T) {
				commEvent := []byte(updateRow)
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				ttl := 10 // 10 seconds
				insertData(t, session, []byte(insertRow), uploadTime, userID)
				err := session.Query(insertQueryWithTTL, commEvent, uploadTime, userID, ttl).Exec()
				if err != nil {
					t.Fatalf("error occurred while Upsert with ttl - %s", err.Error())
				}
				// Validate data immediately
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "Insert with TTL and Timestamp",
			runTest: func(t *testing.T) {
				commEvent := []byte(insertRow)
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				timestamp := time.Now().Add(-1*time.Millisecond).UnixNano() / int64(time.Microsecond)
				ttl := 10 // 10 seconds

				err := session.Query(insertQueryWithTTLAndTS, commEvent, uploadTime, userID, timestamp, ttl).Exec()
				if err != nil {
					t.Fatalf(errorWhileInsertTS, errorWhileInsert, err.Error())
				}
				// Validate data immediately
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "Upsert with TTL and Timestamp",
			runTest: func(t *testing.T) {
				commEvent := []byte(updateRow)
				uploadTime := int64(16278472894)
				userID := uuid.New().String()
				timestamp := time.Now().Add(-10*time.Second).UnixNano() / int64(time.Microsecond)
				ttl := 10 // 10 seconds

				// insert
				err := session.Query(insertQueryWithTTLAndTS, commEvent, uploadTime, userID, timestamp, ttl).Exec()
				if err != nil {
					t.Fatalf(errorWhileInsertTS, errorWhileInsert, err.Error())
				}

				// update
				timestamp += 100
				err = session.Query(insertQueryWithTTLAndTS, commEvent, uploadTime, userID, timestamp, ttl).Exec()
				if err != nil {
					t.Fatalf(errorWhileInsertTS, errorWhileInsert, err.Error())
				}
				// Validate data immediately
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "Partial Insert",
			runTest: func(t *testing.T) {
				uploadTime := int64(1627847296)
				userID := uuid.New().String()
				partialInsertData(t, session, uploadTime, userID)
				commEvent := []byte("")
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "Insert with TTL 0",
			runTest: func(t *testing.T) {
				commEvent := []byte("event with TTL")
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				ttl := 0

				err := session.Query(insertQueryWithTTL, commEvent, uploadTime, userID, ttl).Exec()
				if err != nil {
					t.Fatalf("%s with ttl - %s", errorWhileInsert, err.Error())
				}
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "Upsert with TTL 0 and Timestamp",
			runTest: func(t *testing.T) {
				commEvent := []byte(updateRow)
				uploadTime := int64(16278472894)
				userID := uuid.New().String()
				timestamp := time.Now().Add(-10*time.Second).UnixNano() / int64(time.Microsecond)
				ttl := 0

				// insert
				err := session.Query(insertQueryWithTTLAndTS, commEvent, uploadTime, userID, timestamp, ttl).Exec()
				if err != nil {
					t.Fatalf(errorWhileInsertTS, errorWhileInsert, err.Error())
				}

				// update
				timestamp += 100
				err = session.Query(insertQueryWithTTLAndTS, commEvent, uploadTime, userID, timestamp, ttl).Exec()
				if err != nil {
					t.Fatalf(errorWhileInsertTS, errorWhileInsert, err.Error())
				}
				// Validate data immediately
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.runTest(t)
		})
	}
}

func TestIntegration_Select(t *testing.T) {
	tests := []struct {
		name        string
		runTest     func(t *testing.T)
		expectedErr error
	}{
		{
			name: "count(*)",

			runTest: func(t *testing.T) {
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				var count int64
				insertData(t, session, []byte(insertRow), uploadTime, userID)
				iter := getSelectIter(t, "SELECT count(*) FROM keyspace1.event WHERE upload_time = ? AND user_id = ?", uploadTime, userID)
				defer iter.Close()

				columns := iter.Columns()
				if len(columns) == 0 || columns[0].Name != "count" {
					t.Errorf("expected column name `count` got `%s` ", columns[0].Name)
				}
				if iter.Scan(&count) {
					if count == 0 {
						t.Errorf("expected 1 but got %d", count)
					}
				} else {
					t.Error("error while fetching count value")
				}

			},
		},
		{
			name: "count(*) as myalias",

			runTest: func(t *testing.T) {
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				var count int64
				insertData(t, session, []byte(insertRow), uploadTime, userID)
				iter := getSelectIter(t, "SELECT count(*) as myalias FROM keyspace1.event WHERE upload_time = ? AND user_id = ?", uploadTime, userID)
				defer iter.Close()

				columns := iter.Columns()
				if len(columns) == 0 || columns[0].Name != "myalias" {
					t.Errorf("expected column name `count` got `%s` ", columns[0].Name)
				}
				if iter.Scan(&count) {
					if count == 0 {
						t.Errorf("expected 1 but got %d", count)
					}
				} else {
					t.Error("error while fetching count value")
				}

			},
		},
		{
			name: "count(column)",
			runTest: func(t *testing.T) {
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				var count int64
				insertData(t, session, []byte(insertRow), uploadTime, userID)
				iter := getSelectIter(t, "SELECT count(event_data) FROM keyspace1.event WHERE upload_time = ? AND user_id = ?", uploadTime, userID)
				defer iter.Close()

				columns := iter.Columns()
				if isProxy {
					if len(columns) == 0 || columns[0].Name != "count_event_data" {
						t.Errorf("expected column name `count_event_data` got `%s` ", columns[0].Name)
					}
				} else {
					if len(columns) == 0 || columns[0].Name != "system.count(event_data)" {
						t.Errorf("expected column name `system.count(event_data)` got `%s` ", columns[0].Name)
					}
				}
				if iter.Scan(&count) {
					if count == 0 {
						t.Errorf("expected 1 but got %d", count)
					}
				} else {
					t.Error("error while fetching count value")
				}

			},
		},
		{
			name: "count(column) as myalias",

			runTest: func(t *testing.T) {
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				var count int64
				insertData(t, session, []byte(insertRow), uploadTime, userID)
				iter := getSelectIter(t, "SELECT count(event_data) as myalias FROM keyspace1.event WHERE upload_time = ? AND user_id = ?", uploadTime, userID)
				defer iter.Close()

				columns := iter.Columns()
				if len(columns) == 0 || columns[0].Name != "myalias" {
					t.Errorf("expected column name `myalias` got `%s` ", columns[0].Name)
				}
				if iter.Scan(&count) {
					if count == 0 {
						t.Errorf("expected 1 but got %d", count)
					}
				} else {
					t.Error("error while fetching count value")
				}

			},
		},
		{
			name: "writetime(column)",

			runTest: func(t *testing.T) {
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				var writetime int64
				insertData(t, session, []byte(insertRow), uploadTime, userID)
				iter := getSelectIter(t, "SELECT writetime(event_data) FROM keyspace1.event WHERE upload_time = ? AND user_id = ?", uploadTime, userID)
				defer iter.Close()

				columns := iter.Columns()
				if isProxy {
					if len(columns) == 0 || columns[0].Name != "writetime_event_data" {
						t.Errorf("expected column name `writetime_event_data` got `%s` ", columns[0].Name)
					}
				} else {
					if len(columns) == 0 || columns[0].Name != "writetime(event_data)" {
						t.Errorf("expected column name `writetime(event_data)` got `%s` ", columns[0].Name)
					}
				}
				if iter.Scan(&writetime) {
					if writetime <= uploadTime {
						t.Errorf("expected current_unix_time but got %d", writetime)
					}
				} else {
					t.Error("error while fetching writetime value")
				}

			},
		},
		{
			name: "writetime(column) as wt",

			runTest: func(t *testing.T) {
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				var writetime int64
				insertData(t, session, []byte(insertRow), uploadTime, userID)
				iter := getSelectIter(t, "SELECT writetime(event_data) as wt FROM keyspace1.event WHERE upload_time = ? AND user_id = ?", uploadTime, userID)
				defer iter.Close()

				columns := iter.Columns()
				if len(columns) == 0 || columns[0].Name != "wt" {
					t.Errorf("expected column name `wt` got `%s` ", columns[0].Name)
				}
				if iter.Scan(&writetime) {
					if writetime <= uploadTime {
						t.Errorf("expected current_unix_time but got %d", writetime)
					}
				} else {
					t.Error("error while fetching writetime value")
				}

			},
		},
		{
			name: "UnsupportedFunc(column)",
			runTest: func(t *testing.T) {
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				insertData(t, session, []byte(insertRow), uploadTime, userID)
				query := "SELECT xxxx(event_data) FROM keyspace1.event WHERE upload_time = ? AND user_id = ?"
				iter := session.Query(query, uploadTime, userID).Iter()

				// Check if there is an error in preparing or executing the query
				if err := iter.Close(); err == nil {
					t.Fatalf("Expected query to fail but it succeeded")
				} else {
					expectedErrMsg := "Unknown function 'xxxx'"
					if err.Error() != expectedErrMsg {
						t.Fatalf("Expected error message '%s' but got '%s'", expectedErrMsg, err.Error())
					} else {
						t.Logf("Query failed as expected with error: %v", err)
					}
				}

			},
		},
		{
			name: "Select col1, col2 from table",
			runTest: func(t *testing.T) {
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				commEventInsert := []byte(insertRow)

				var retrievedUploadTime int64
				var commEvent []byte

				insertData(t, session, commEventInsert, uploadTime, userID)

				query := "SELECT upload_time, event_data FROM keyspace1.event WHERE upload_time = ? AND user_id = ?"
				iter := session.Query(query, uploadTime, userID).Iter()

				if !iter.Scan(&retrievedUploadTime, &commEvent) {
					if err := iter.Close(); err != nil {
						t.Fatalf(errorInSelect, err)
					}
					t.Fatalf("No rows returned or error in scanning")
				}
				if retrievedUploadTime != uploadTime {
					t.Errorf("Expected upload_time %d but got %d", uploadTime, retrievedUploadTime)
				}

				if string(commEvent) != string(commEventInsert) {
					t.Errorf("Expected event_data %s but got %s", commEventInsert, commEvent)
				}
			},
		},
		{
			name: "Limit ",
			runTest: func(t *testing.T) {
				var limit = 5
				iter := session.Query(basicSelectWithLimit, limit).Iter()

				if iter.NumRows() < limit || iter.NumRows() == 0 {
					t.Fatalf("Expected number of rows >0 and  <5 but got %d", iter.NumRows())
				}

				if err := iter.Close(); err != nil {
					t.Fatalf(errorInSelect, err)
				}
			},
		},
		{
			name: "Select with allow filtering",
			runTest: func(t *testing.T) {
				uploadTime := int64(1627847264)
				query := "SELECT * FROM keyspace1.event where upload_time > ? ALLOW FILTERING;"
				iter := session.Query(query, uploadTime).Iter()

				if iter.NumRows() == 0 {
					t.Fatalf("Expected number of rows >0 but got %d", iter.NumRows())
				}

				if err := iter.Close(); err != nil {
					t.Fatalf(errorInSelect, err)
				}
			},
		},
		{
			name: "Order by Desc",
			runTest: func(t *testing.T) {
				commEvent := []byte(insertRow)
				userID := uuid.New().String()

				insertData(t, session, commEvent, int64(1627847284), userID)
				insertData(t, session, commEvent, int64(1627847286), userID)
				insertData(t, session, commEvent, int64(1627847288), userID)

				query := "SELECT upload_time FROM keyspace1.event WHERE user_id = ? ORDER BY upload_time DESC;"
				iter := session.Query(query, userID).Iter()
				var uploadTime int64
				var previousUploadTime int64 = int64(1<<63 - 1) // Initialize with the maximum possible int64 value
				for iter.Scan(&uploadTime) {
					if uploadTime > previousUploadTime {
						t.Fatalf("Result is not in descending order: %d came after %d", uploadTime, previousUploadTime)
					}
					previousUploadTime = uploadTime
				}
				if iter.NumRows() == 0 {
					t.Fatalf("Expected number of rows > 0 but got %d", iter.NumRows())
				}
				if err := iter.Close(); err != nil {
					t.Fatalf(errorInSelect, err)
				}
			},
		},
		{
			name: "Order by ASC",
			runTest: func(t *testing.T) {
				var uploadTime int64
				userID := uuid.New().String()
				commEvent := []byte(insertRow)

				insertData(t, session, commEvent, int64(1627847284), userID)
				insertData(t, session, commEvent, int64(1627847286), userID)
				insertData(t, session, commEvent, int64(1627847288), userID)

				query := "SELECT upload_time FROM keyspace1.event WHERE user_id = ? ORDER BY upload_time ASC;"
				iter := session.Query(query, userID).Iter()

				var previousUploadTime int64 = 1
				for iter.Scan(&uploadTime) {
					if uploadTime < previousUploadTime {
						t.Fatalf("Result is not in ascending order: %d came after %d", uploadTime, previousUploadTime)
					}
					previousUploadTime = uploadTime
				}
				if iter.NumRows() == 0 {
					t.Fatalf("Expected number of rows > 0 but got %d", iter.NumRows())
				}
				if err := iter.Close(); err != nil {
					t.Fatalf(errorInSelect, err)
				}
			},
		},
		{
			name: "Stale Read",
			runTest: func(t *testing.T) {
				uploadTime := int64(1627847284)
				userID := uuid.New().String()
				commEventInsert := []byte(insertRow)

				var retrievedUploadTime int64
				var commEvent []byte

				insertData(t, session, commEventInsert, uploadTime, userID)

				time.Sleep(time.Second)

				customPayload := map[string][]byte{
					"spanner_maxstaleness": []byte("1s"),
				}
				query := "SELECT upload_time, event_data FROM keyspace1.event WHERE upload_time = ? AND user_id = ?"
				stmt := session.Query(query, uploadTime, userID)
				stmt.CustomPayload(customPayload)

				iter := stmt.Iter()

				if !iter.Scan(&retrievedUploadTime, &commEvent) {
					if err := iter.Close(); err != nil {
						t.Fatalf(errorInSelect, err)
					}
					t.Fatalf("No rows returned or error in scanning")
				}
				if retrievedUploadTime != uploadTime {
					t.Errorf("Expected upload_time %d but got %d", uploadTime, retrievedUploadTime)
				}

				if string(commEvent) != string(commEventInsert) {
					t.Errorf("Expected event_data %s but got %s", commEventInsert, commEvent)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.runTest(t)
		})
	}
}

func getSelectIter(t *testing.T, query string, args ...interface{}) *gocql.Iter {
	iter := session.Query(query, args...).Iter()
	if iter.NumRows() == 0 {
		t.Fatalf("no rows returned")
	}
	return iter
}

func TestIntegration_Update(t *testing.T) {
	tests := []struct {
		name        string
		runTest     func(t *testing.T)
		expectedErr error
	}{
		{
			name: "Update",
			runTest: func(t *testing.T) {
				commEvent := []byte(updateRow)
				uploadTime := int64(1627847296)
				userID := uuid.New().String()
				insertData(t, session, []byte(insertRow), uploadTime, userID)
				updateData(t, session, commEvent, uploadTime, userID)
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "TTL",
			runTest: func(t *testing.T) {
				commEvent := []byte(insertRow)
				uploadTime := int64(1627847296)
				userID := uuid.New().String()
				ttl := 20

				insertData(t, session, commEvent, uploadTime, userID)

				commEvent = []byte(updateRow)
				if err := session.Query(updateQueryWithTTL, ttl, commEvent, uploadTime, userID).Exec(); err != nil {
					t.Fatalf(failedToUpdateRow, err)
				}
				commEvent = []byte(updateRow)
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "TTL with 0",
			runTest: func(t *testing.T) {
				commEvent := []byte(insertRow)
				uploadTime := int64(1627847296)
				userID := uuid.New().String()
				ttl := 0

				insertData(t, session, commEvent, uploadTime, userID)

				commEvent = []byte(updateRow)
				if err := session.Query(updateQueryWithTTL, ttl, commEvent, uploadTime, userID).Exec(); err != nil {
					t.Fatalf(failedToUpdateRow, err)
				}
				commEvent = []byte(updateRow)
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "Update with timestamp val > last update time & ttl  ",
			runTest: func(t *testing.T) {
				commEvent := []byte(insertRow)
				uploadTime := int64(1627847296)
				userID := uuid.New().String()
				ttl := 2000
				currentTime := int64(1627847296)

				err := session.Query(insertQueryWithTimestamp, commEvent, uploadTime, userID, currentTime).Exec()
				if err != nil {
					t.Error("insert failed")
				}

				currentTime = int64(1627947299)
				commEvent = []byte(updateRow)

				if err := session.Query(updateQueryWithTTLAndTS, ttl, currentTime, commEvent, uploadTime, userID).Exec(); err != nil {
					t.Fatalf(failedToUpdateRow, err)
				}
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "Update with timestamp val < last update time & ttl ",
			runTest: func(t *testing.T) {
				commEvent := []byte(insertRow)
				uploadTime := int64(1627847296)
				userID := uuid.New().String()
				ttl := 20
				insertData(t, session, commEvent, uploadTime, userID)

				currentTime := time.Now().Add(-2*time.Minute).UnixNano() / int64(time.Microsecond)

				if err := session.Query(updateQueryWithTTLAndTS, ttl, currentTime, []byte(updateRow), uploadTime, userID).Exec(); err != nil {
					t.Fatalf(failedToUpdateRow, err)
				}
				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "Update query with using timestamp in future should raise error ",
			runTest: func(t *testing.T) {
				uploadTime := int64(1627847296)
				userID := uuid.New().String()

				insertData(t, session, []byte(insertRow), uploadTime, userID)

				commEvent := []byte(updateRow)
				ttl := 20

				currentTime := time.Now().Add(1*time.Hour).UnixNano() / int64(time.Microsecond)
				if err := session.Query(updateQueryWithTTLAndTS, ttl, currentTime, commEvent, uploadTime, userID).Exec(); err != nil {
					t.Logf("%s: %v", updateInFutureError, err)

				} else if isProxy {
					t.Error(updateInFutureError)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.runTest(t)
		})
	}
}

func TestIntegration_Delete(t *testing.T) {
	tests := []struct {
		name        string
		runTest     func(t *testing.T)
		expectedErr error
	}{
		{
			name: "Single row delete followed by Select & validate",
			runTest: func(t *testing.T) {
				uploadTime := int64(1627847284)
				userID := uuid.New().String()

				insertData(t, session, []byte(insertRow), uploadTime, userID)

				if err := session.Query("delete from keyspace1.event WHERE upload_time = ? AND user_id = ?", uploadTime, userID).Exec(); err != nil {
					t.Fatalf("failed to delete row: %v", err)
				}
				var retrievedCommEvent []byte
				var retrievedUploadTime int64
				var retrievedUserID string
				err := session.Query(selectQuery, uploadTime, userID).Scan(&retrievedCommEvent, &retrievedUploadTime, &retrievedUserID)
				if err == nil {
					t.Fatalf("expected no data after delete, but data found - %v %v %v", retrievedCommEvent, retrievedUploadTime, retrievedUserID)
				}

			},
		},
		{
			name: "Range delete followed by Select & validate",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()

				insertData(t, session, []byte(insertRow), int64(1627847284), userID)
				insertData(t, session, []byte(insertRow), int64(1627847288), userID)
				insertData(t, session, []byte(insertRow), int64(1627847294), userID)

				if err := session.Query("delete from keyspace1.event WHERE user_id = ?", userID).Exec(); err != nil {
					t.Fatalf("failed to delete row: %v", err)
				}
				var retrievedCommEvent []byte
				var retrievedUploadTime int64
				var retrievedUserID string
				err := session.Query("SELECT event_data, upload_time, user_id FROM keyspace1.event WHERE user_id = ?", userID).Scan(&retrievedCommEvent, &retrievedUploadTime, &retrievedUserID)
				if err == nil {
					t.Errorf("expected no data after delete, but data found - %v %v %v", retrievedCommEvent, retrievedUploadTime, retrievedUserID)
				}

			},
		},
		{
			name: "Delete with timestamp of future [composite key]",
			runTest: func(t *testing.T) {
				commEvent := []byte(insertRow)
				uploadTime := int64(1627847296)
				userID := uuid.New().String()

				insertData(t, session, commEvent, uploadTime, userID)

				futureTimestamp := time.Now().Add(time.Hour).UnixNano() / int64(time.Microsecond)

				err := session.Query(deleteWithTS, futureTimestamp, uploadTime, userID).Exec()
				if err != nil {
					t.Fatalf(errorWhileDelete, err.Error())
				}

				iter := session.Query(selectQuery, uploadTime, userID).Iter()
				if iter.NumRows() > 0 {
					t.Error(expectedZeroRows)
				}
			},
		},
		{
			name: "Delete with timestamp of future [range]",
			runTest: func(t *testing.T) {

				commEvent := []byte(insertRow)
				uploadTime := int64(1627847296)
				userID := uuid.New().String()

				insertData(t, session, commEvent, uploadTime, userID)

				futureTimestamp := time.Now().Add(time.Hour).UnixNano() / int64(time.Microsecond)

				err := session.Query(deleteWithTS, futureTimestamp, uploadTime, userID).Exec()
				if err != nil {
					t.Errorf(errorWhileDelete, err.Error())
				}

				iter := session.Query(selectQuery, uploadTime, userID).Iter()
				if iter.NumRows() > 0 {
					t.Error(expectedZeroRows)
				}
			},
		},
		{
			name: "Delete with timestamp < last_commit_ts (delete should not reflect) [composite key]",
			runTest: func(t *testing.T) {
				commEvent := []byte(insertRow)
				uploadTime := int64(1627847296)
				userID := uuid.New().String()

				insertData(t, session, commEvent, uploadTime, userID)

				futureTimestamp := time.Now().Add(-1*time.Minute).UnixNano() / int64(time.Microsecond)

				err := session.Query(deleteWithTS, futureTimestamp, uploadTime, userID).Exec()
				if err != nil {
					t.Fatalf(errorWhileDelete, err.Error())
				}

				iter := session.Query(selectQuery, uploadTime, userID).Iter()
				if iter.NumRows() == 0 {
					t.Error("Expected rows but got 0")
				}
			},
		},
		{

			runTest: func(t *testing.T) {
				commEvent := []byte(insertRow)
				uploadTime := int64(1627847296)
				userID := uuid.New().String()

				insertData(t, session, commEvent, uploadTime, userID)

				futureTimestamp := time.Now().Add(time.Hour).UnixNano() / int64(time.Microsecond)

				err := session.Query(rangeDeleteWithTS, futureTimestamp, userID).Exec()
				if err != nil {
					t.Errorf(errorWhileDelete, err.Error())
				}

				iter := session.Query(selectQuery, uploadTime, userID).Iter()
				if iter.NumRows() > 0 {
					t.Error(expectedZeroRows)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.runTest(t)
		})
	}
}

func TestIntegration_BasicBatch(t *testing.T) {
	tests := []struct {
		name        string
		runTest     func(t *testing.T)
		expectedErr error
	}{
		{
			name: "Basic Batch insert on same composite key",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQuery, []byte("Insert Row 1"), uploadTime, userID)
				batch.Query(insertQuery, []byte("Insert Row 2"), uploadTime, userID)
				batch.Query(insertQuery, []byte("Insert Row 3"), uploadTime, userID)

				handleBatchExecution(t, batch)

				selectAndValidateData(t, session, []byte("Insert Row 3"), uploadTime, userID)
			},
		},
		{
			name: "Basic Batch insert on different composite key",
			runTest: func(t *testing.T) {

				userID := uuid.New().String()

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQuery, []byte("Insert Row 1"), int64(1627847284), userID)
				batch.Query(insertQuery, []byte("Insert Row 2"), int64(1627847286), userID)
				batch.Query(insertQuery, []byte("Insert Row 3"), int64(1627847288), userID)

				handleBatchExecution(t, batch)

				iter := session.Query(selectColWithOrderBy, userID).Iter()

				var uploadTime int64
				var commEvent []byte
				expectedResults := []struct {
					uploadTime int64
					commEvent  string
				}{
					{1627847284, "Insert Row 1"},
					{1627847286, "Insert Row 2"},
					{1627847288, "Insert Row 3"},
				}
				i := 0

				for iter.Scan(&uploadTime, &commEvent) {
					if i >= len(expectedResults) {
						t.Fatal(errorMoreRowsThanExpected)
					}
					if uploadTime != expectedResults[i].uploadTime || string(commEvent) != expectedResults[i].commEvent {
						t.Fatalf(unexpectedRow, i, uploadTime, commEvent, expectedResults[i].uploadTime, expectedResults[i].commEvent)
					}
					i++
				}
				if i != len(expectedResults) {
					t.Fatalf(errorExpectedRows, len(expectedResults), i)
				}

				if err := iter.Close(); err != nil {
					t.Fatalf(errorInClosingIter, err)
				}
			},
		},
		{
			name: "Range Delete followed by insert on same composite key",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				commEvent := []byte("Insert Row 3")
				uploadTime := int64(1627847284)

				// Range delete
				if err := session.Query("DELETE FROM keyspace1.event WHERE user_id = ? ", userID).Exec(); err != nil {
					t.Errorf(errorWhileDelete, err.Error())
				}

				// Batch insert
				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQuery, []byte("Insert Row 1"), uploadTime, userID)
				batch.Query(insertQuery, []byte("Insert Row 2"), uploadTime, userID)
				batch.Query(insertQuery, commEvent, uploadTime, userID)

				handleBatchExecution(t, batch)

				selectAndValidateData(t, session, commEvent, uploadTime, userID)
			},
		},
		{
			name: "Range Delete followed by insert on different composite key",
			runTest: func(t *testing.T) {

				userID := uuid.New().String()

				// Range delete
				if err := session.Query("DELETE FROM keyspace1.event WHERE user_id = ?", userID).Exec(); err != nil {
					t.Errorf(errorWhileDelete, err.Error())
				}

				// Batch insert
				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQuery, []byte("Insert Row 1"), int64(1627847284), userID)
				batch.Query(insertQuery, []byte("Insert Row 2"), int64(1627847286), userID)
				batch.Query(insertQuery, []byte("Insert Row 3"), int64(1627847288), userID)

				handleBatchExecution(t, batch)

				iter := session.Query(selectColWithOrderBy, userID).Iter()

				var uploadTime int64
				var commEvent []byte
				expectedResults := []struct {
					uploadTime int64
					commEvent  string
				}{
					{1627847284, "Insert Row 1"},
					{1627847286, "Insert Row 2"},
					{1627847288, "Insert Row 3"},
				}
				i := 0

				for iter.Scan(&uploadTime, &commEvent) {
					if i >= len(expectedResults) {
						t.Fatal(errorMoreRowsThanExpected)
					}
					if uploadTime != expectedResults[i].uploadTime || string(commEvent) != expectedResults[i].commEvent {
						t.Fatalf(unexpectedRow, i, uploadTime, commEvent, expectedResults[i].uploadTime, expectedResults[i].commEvent)
					}
					i++
				}
				if i != len(expectedResults) {
					t.Fatalf(errorExpectedRows, len(expectedResults), i)
				}

				if err := iter.Close(); err != nil {
					t.Fatalf(errorInClosingIter, err)
				}
			},
		},
		//TODO - working wih proxy but failing with cassandra diff got this insert Row" expected "Updated Row"
		// {
		// 	name: "Scenario 36 - Simple insert and update on composite key",

		// 	runTest: func(t *testing.T) {
		// 		userID := uuid.New().String()
		// 		uploadTime := int64(1627847284)

		// 		batch := session.NewBatch(gocql.LoggedBatch)
		// 		batch.Query(insertQuery, []byte(insertRow), uploadTime, userID)
		// 		batch.Query(insertQuery, []byte(insertRow), uploadTime, userID)
		// 		batch.Query(updateQuery, []byte(updateRow), uploadTime, userID)

		// 		handleBatchExecution(t, batch)

		// 		commEvent := []byte(updateRow)
		// 		selectAndValidateData(t, session, commEvent, uploadTime, userID)
		// 	},
		// },
		{
			name: "Scenario 37 - Simple insert and update on different composite key",
			runTest: func(t *testing.T) {
				userID1 := uuid.New().String()
				uploadTime1 := int64(1627847284)
				userID2 := uuid.New().String()
				uploadTime2 := int64(1627847286)

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQuery, []byte("Insert Row 1"), uploadTime1, userID1)
				batch.Query(insertQuery, []byte("Insert Row 2"), uploadTime2, userID2)
				batch.Query(updateQuery, []byte("Updated Row 2"), uploadTime2, userID2)

				handleBatchExecution(t, batch)

				// Select and validate the first insert
				var commEvent []byte
				if err := session.Query("SELECT event_data FROM keyspace1.event WHERE user_id = ? AND upload_time = ?", userID1, uploadTime1).Scan(&commEvent); err != nil {
					t.Fatalf("Select failed: %v", err)
				}
				if string(commEvent) != "Insert Row 1" {
					t.Fatalf("Expected event_data to be 'Insert Row 1' but got '%s'", commEvent)
				}

				// Select and validate the update
				if err := session.Query("SELECT event_data FROM keyspace1.event WHERE user_id = ? AND upload_time = ?", userID2, uploadTime2).Scan(&commEvent); err != nil {
					t.Fatalf("Select failed: %v", err)
				}
				if string(commEvent) != "Updated Row 2" {
					t.Fatalf("Expected event_data to be 'Updated Row 2' but got '%s'", commEvent)
				}
			},
		},
		{
			name: "Simple insert, update and delete on same composite key",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQuery, []byte(insertRow), uploadTime, userID)
				batch.Query(updateQuery, []byte("Updated Row"), uploadTime, userID)
				batch.Query("DELETE FROM keyspace1.event WHERE user_id = ? AND upload_time = ?", userID, uploadTime)

				handleBatchExecution(t, batch)

				// Validate the deletion
				var commEvent []byte
				if err := session.Query("SELECT event_data FROM keyspace1.event WHERE user_id = ? AND upload_time = ?", userID, uploadTime).Scan(&commEvent); err == nil {
					t.Fatalf("Expected row to be deleted but found event_data: %s", commEvent)
				}
			},
		},
		{
			name: "Scenario - Simple insert on different composite key and range delete using range condition",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQuery, []byte("Insert Row 1"), int64(1627847284), userID)
				batch.Query(insertQuery, []byte("Insert Row 2"), int64(1627847286), userID)
				batch.Query(insertQuery, []byte("Insert Row 3"), int64(1627847288), userID)
				batch.Query("DELETE FROM keyspace1.event WHERE user_id = ? AND upload_time >= ? AND upload_time <= ?", userID, int64(1627847270), int64(1627847390))

				handleBatchExecution(t, batch)

				iter := session.Query(selectColWithOrderBy, userID).Iter()

				if iter.NumRows() != 0 {
					t.Fatalf("Expected all rows to be deleted but found %d rows", iter.NumRows())
				}
				if err := iter.Close(); err != nil {
					t.Fatalf("Error closing iterator after delete: %v", err)
				}
			},
		},
		{
			name: "Simple insert on different composite key and range delete",
			runTest: func(t *testing.T) {

				userID := uuid.New().String()

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQuery, []byte("Insert Row 1"), int64(1627847284), userID)
				batch.Query(insertQuery, []byte("Insert Row 2"), int64(1627847286), userID)
				batch.Query(insertQuery, []byte("Insert Row 3"), int64(1627847288), userID)
				batch.Query("DELETE FROM keyspace1.event WHERE user_id = ?", userID)

				handleBatchExecution(t, batch)

				iter := session.Query(selectColWithOrderBy, userID).Iter()

				if iter.NumRows() != 0 {
					t.Fatalf("Expected all rows to be deleted but found %d rows", iter.NumRows())
				}
				if err := iter.Close(); err != nil {
					t.Fatalf("Error closing iterator after delete: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.runTest(t)
		})
	}
}

func TestIntegration_BatchWithTS(t *testing.T) {
	tests := []struct {
		name        string
		runTest     func(t *testing.T)
		expectedErr error
	}{
		{
			name: "Scenario 41 - Batch insert on same composite key and same timestamp",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				timestamp := int64(1627847284)

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 1"), uploadTime, userID, timestamp)
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 2"), uploadTime, userID, timestamp)
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 3"), uploadTime, userID, timestamp)

				err := session.ExecuteBatch(batch)
				if err != nil {
					t.Errorf(errorWhileExecutingBatch, err.Error())
				}

				selectAndValidateData(t, session, []byte("Insert Row 3"), uploadTime, userID)
			},
		},
		{
			name: "Batch insert on same composite key and different timestamp",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 1"), int64(1627847284), userID, int64(1627847284))
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 2"), int64(1627847285), userID, int64(1627847285))
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 3"), int64(1627847286), userID, int64(1627847286))

				handleBatchExecution(t, batch)

				selectAndValidateMultipleData(t, session, userID, []struct {
					uploadTime int64
					commEvent  string
				}{
					{1627847284, "Insert Row 1"},
					{1627847285, "Insert Row 2"},
					{1627847286, "Insert Row 3"},
				})
			},
		},
		{
			name: "Insert on different composite key and same timestamp",
			runTest: func(t *testing.T) {
				userID1 := uuid.New().String()
				userID2 := uuid.New().String()
				uploadTime := int64(1627847284)
				timestamp := int64(1627847284)

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 1"), uploadTime, userID1, timestamp)
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 2"), uploadTime, userID2, timestamp)

				handleBatchExecution(t, batch)

				selectAndValidateData(t, session, []byte("Insert Row 1"), uploadTime, userID1)
				selectAndValidateData(t, session, []byte("Insert Row 2"), uploadTime, userID2)
			},
		},
		{
			name: "Insert on different composite key and different timestamp",
			runTest: func(t *testing.T) {
				userID1 := uuid.New().String()
				userID2 := uuid.New().String()

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 1"), int64(1627847284), userID1, int64(1627847284))
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 2"), int64(1627847285), userID2, int64(1627847285))

				handleBatchExecution(t, batch)

				selectAndValidateData(t, session, []byte("Insert Row 1"), int64(1627847284), userID1)
				selectAndValidateData(t, session, []byte("Insert Row 2"), int64(1627847285), userID2)
			},
		},
		{
			name: "Range Delete timestamp followed by insert on same composite key [delete with lower timestamp]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				timestamp := int64(1627848284)

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(deleteWithTS, timestamp-1000, int64(1627847280), userID)
				batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, timestamp)

				handleBatchExecution(t, batch)

				selectAndValidateData(t, session, []byte(insertRow), uploadTime, userID)
			},
		},
		{
			name: "Range Delete timestamp followed by insert on same composite key [delete with higher timestamp]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				timestamp := int64(1627847284)

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(rangeDeleteWithTS, timestamp+1000, userID)
				batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, timestamp)

				handleBatchExecution(t, batch)

				ValidateNoRowsFound(t, session, userID)
			},
		},
		{
			name: "Insert followed by delete on same PK [delete with lower timestamp]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				insertTimestamp := int64(1720445570000)
				deleteTimestamp := insertTimestamp - 1000

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
				batch.Query(deleteWithTS, deleteTimestamp, uploadTime, userID)

				handleBatchExecution(t, batch)

				selectAndValidateData(t, session, []byte(insertRow), uploadTime, userID)
			},
		},

		{
			name: "Insert followed by delete on different PK [delete with lower timestamp]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				insertTimestamp := int64(1720445570000)
				deleteTimestamp := insertTimestamp - 10000

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 1"), uploadTime, userID, insertTimestamp)
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 2"), uploadTime+10, userID, insertTimestamp)
				batch.Query(rangeDeleteWithTS, deleteTimestamp, userID)

				handleBatchExecution(t, batch)

				selectAndValidateData(t, session, []byte("Insert Row 1"), uploadTime, userID)
				selectAndValidateData(t, session, []byte("Insert Row 2"), uploadTime+10, userID)
			},
		},
		{
			name: "Insert followed by delete on same PK [delete with higher timestamp]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				insertTimestamp := int64(1627847284)
				deleteTimestamp := int64(1627847700)

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
				batch.Query(deleteWithTS, deleteTimestamp, uploadTime, userID)

				handleBatchExecution(t, batch)
				ValidateNoRowsFound(t, session, userID)
			},
		},
		{
			name: "Insert followed by delete on different composite [delete with higher timestamp]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime1 := int64(1627847284)
				insertTimestamp1 := int64(1627847285)
				uploadTime2 := int64(1627847288)
				deleteTimestamp2 := int64(1627857183)

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 1"), uploadTime1, userID, insertTimestamp1)
				batch.Query(insertQueryWithTimestamp, []byte("Insert Row 2"), uploadTime2, userID, insertTimestamp1)
				batch.Query(rangeDeleteWithTS, deleteTimestamp2, userID)

				handleBatchExecution(t, batch)
				ValidateNoRowsFound(t, session, userID)
			},
		},
		{
			name: "Batch Insert[t1] -> update [t1+100] -> delete [t1+200]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				insertTimestamp := int64(1720445570000)
				updateTimestamp := insertTimestamp + 100
				deleteTimestamp := insertTimestamp + 200

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
				batch.Query(updateQueryWithTTLAndTS, 100, updateTimestamp, []byte(updateRow), uploadTime, userID)
				batch.Query(deleteWithTS, deleteTimestamp, uploadTime, userID)

				handleBatchExecution(t, batch)

				ValidateNoRowsFound(t, session, userID)
			},
		},
		//TODO - Cassandra Error - Expected 0 rows but got 1 [reason - update also insert if row not found]
		// {
		// 	name: "Batch Insert[t1] -> update [t1+100] -> delete [t1+50]",
		// 	runTest: func(t *testing.T) {
		// 		userID := uuid.New().String()
		// 		uploadTime := int64(1627847284)
		// 		insertTimestamp := int64(1720445570000)
		// 		updateTimestamp := insertTimestamp + 100
		// 		deleteTimestamp := insertTimestamp + 50

		// 		batch := session.NewBatch(gocql.LoggedBatch)
		// 		batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
		// 		batch.Query(updateQueryWithTTLAndTS, 100, updateTimestamp, []byte(updateRow), uploadTime, userID)
		// 		batch.Query(deleteWithTS, deleteTimestamp, uploadTime, userID)

		// 		handleBatchExecution(t, batch)

		// 		// Query Execution happen in order of batch hence update will  be applied and delete will be ignored
		// 		selectAndValidateData(t, session, []byte(updateRow), uploadTime, userID)
		// 	},
		// },
		{
			name: "Batch Insert[t1] -> update [t1+100] -> delete [t1-100]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				insertTimestamp := int64(1720445570000)
				updateTimestamp := insertTimestamp + 100
				deleteTimestamp := insertTimestamp - 100

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
				batch.Query(updateQueryWithTTLAndTS, 100, updateTimestamp, []byte(updateRow), uploadTime, userID)
				batch.Query(deleteWithTS, deleteTimestamp, uploadTime, userID)

				handleBatchExecution(t, batch)

				selectAndValidateData(t, session, []byte(updateRow), uploadTime, userID)
			},
		},
		//TODO - Cassandra error - Expected 0 rows but got 1 [Reason cassandra insert if row not found for update]
		// {
		// 	name: "Batch  insert[t1] -> delete[t1+50] -> update[t1+100]",
		// 	runTest: func(t *testing.T) {
		// 		userID := uuid.New().String()
		// 		uploadTime := int64(1627847284)
		// 		insertTimestamp := int64(1720445570000)
		// 		updateTimestamp := insertTimestamp + 100
		// 		deleteTimestamp := insertTimestamp + 50

		// 		batch := session.NewBatch(gocql.LoggedBatch)
		// 		batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
		// 		batch.Query(deleteWithTS, deleteTimestamp, uploadTime, userID)
		// 		batch.Query(updateQueryWithTTLAndTS, 100, updateTimestamp, []byte(updateRow), uploadTime, userID)

		// 		handleBatchExecution(t, batch)
		// 		ValidateNoRowsFound(t, session, userID)
		// 	},
		// },
		{
			name: "Batch  insert[t1] -> delete[t1+200] -> update[t1+100]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				insertTimestamp := int64(1720445570000)
				updateTimestamp := insertTimestamp + 100
				deleteTimestamp := insertTimestamp + 200

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
				batch.Query(deleteWithTS, deleteTimestamp, uploadTime, userID)
				batch.Query(updateQueryWithTTLAndTS, 100, updateTimestamp, []byte(updateRow), uploadTime, userID)

				handleBatchExecution(t, batch)
				ValidateNoRowsFound(t, session, userID)
			},
		},
		{
			name: "Batch  insert[t1] -> delete[t1-100] -> update[t1+100]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				insertTimestamp := int64(1720445570000)
				updateTimestamp := insertTimestamp + 100
				deleteTimestamp := insertTimestamp - 100

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
				batch.Query(deleteWithTS, deleteTimestamp, uploadTime, userID)
				batch.Query(updateQueryWithTTLAndTS, 100, updateTimestamp, []byte(updateRow), uploadTime, userID)

				handleBatchExecution(t, batch)

				// delete will be ignored as insert with higher timestamp exist
				selectAndValidateData(t, session, []byte(updateRow), uploadTime, userID)
			},
		},
		{
			name: "Batch delete[t1-100] -> insert[t1]  -> update[t1+10]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				insertTimestamp := int64(1720445570000)
				updateTimestamp := insertTimestamp + 10
				deleteTimestamp := insertTimestamp - 100

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(deleteWithTS, deleteTimestamp, uploadTime, userID)
				batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
				batch.Query(updateQueryWithTTLAndTS, 100, updateTimestamp, []byte(updateRow), uploadTime, userID)

				handleBatchExecution(t, batch)
				// delete will be ignored as insert with higher timestamp exist
				selectAndValidateData(t, session, []byte(updateRow), uploadTime, userID)
			},
		},
		//TODO - cassandra error - Expected 0 rows but got 1 [Reason cassandra delete]
		// {
		// 	name: "Batch delete[t1 + 100] -> insert[t1]  -> update[t1+200]",
		// 	runTest: func(t *testing.T) {
		// 		userID := uuid.New().String()
		// 		uploadTime := int64(1627847284)
		// 		insertTimestamp := int64(1720445570000)
		// 		updateTimestamp := insertTimestamp + 200
		// 		deleteTimestamp := insertTimestamp + 100

		// 		batch := session.NewBatch(gocql.LoggedBatch)
		// 		batch.Query(deleteWithTS, deleteTimestamp, uploadTime, userID)
		// 		batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
		// 		batch.Query(updateQueryWithTTLAndTS, 100, updateTimestamp, []byte(updateRow), uploadTime, userID)

		// 		handleBatchExecution(t, batch)
		// 		ValidateNoRowsFound(t, session, userID)
		// 	},
		// },
		{
			name: "Batch delete[t1 + 200] -> insert[t1]  -> update[t1+100]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				insertTimestamp := int64(1720445570000)
				updateTimestamp := insertTimestamp + 100
				deleteTimestamp := insertTimestamp + 200

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(deleteWithTS, deleteTimestamp, uploadTime, userID)
				batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
				batch.Query(updateQueryWithTTLAndTS, 100, updateTimestamp, []byte(updateRow), uploadTime, userID)

				handleBatchExecution(t, batch)
				ValidateNoRowsFound(t, session, userID)
			},
		},
		{
			name: "Batch delete[t1-100] -> insert[t1]  -> update[t1+100]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				insertTimestamp := int64(1720445570000)
				updateTimestamp := insertTimestamp + 100
				deleteTimestamp := insertTimestamp - 100

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(rangeDeleteWithTS, deleteTimestamp, userID)
				batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
				batch.Query(updateQueryWithTTLAndTS, 100, updateTimestamp, []byte(updateRow), uploadTime, userID)

				handleBatchExecution(t, batch)
				// delete will be ignored as insert with higher timestamp exist
				selectAndValidateData(t, session, []byte(updateRow), uploadTime, userID)
			},
		},
		//TODO - Cassandra error - Expected 0 rows but got 1 [Reason cassandra insert if row not found for update]
		// {
		// 	name: "Batch delete[t1+100] -> insert[t1]  -> update[t1+200]",
		// 	runTest: func(t *testing.T) {
		// 		userID := uuid.New().String()
		// 		uploadTime := int64(1627847284)
		// 		insertTimestamp := int64(1720445570000)
		// 		updateTimestamp := insertTimestamp + 200
		// 		deleteTimestamp := insertTimestamp + 100

		// 		batch := session.NewBatch(gocql.LoggedBatch)
		// 		batch.Query(rangeDeleteWithTS, deleteTimestamp, userID)
		// 		batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
		// 		batch.Query(updateQueryWithTTLAndTS, 100, updateTimestamp, []byte(updateRow), uploadTime, userID)

		// 		handleBatchExecution(t, batch)
		// 		ValidateNoRowsFound(t, session, userID)
		// 	},
		// },
		{
			name: "Batch delete[t1 + 200] -> insert[t1]  -> update[t1+100]",
			runTest: func(t *testing.T) {
				userID := uuid.New().String()
				uploadTime := int64(1627847284)
				insertTimestamp := int64(1720445570000)
				updateTimestamp := insertTimestamp + 100
				deleteTimestamp := insertTimestamp + 200

				batch := session.NewBatch(gocql.LoggedBatch)
				batch.Query(deleteWithTS, deleteTimestamp, uploadTime, userID)
				batch.Query(insertQueryWithTimestamp, []byte(insertRow), uploadTime, userID, insertTimestamp)
				batch.Query(updateQueryWithTTLAndTS, 100, updateTimestamp, []byte(updateRow), uploadTime, userID)

				handleBatchExecution(t, batch)
				ValidateNoRowsFound(t, session, userID)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.runTest(t)
		})
	}
}

func ValidateNoRowsFound(t *testing.T, session *gocql.Session, userId string) {
	iter := session.Query(rangeSelect, userId).Iter()

	if iter.NumRows() > 0 {
		t.Errorf("Expected 0 rows but got %d", iter.NumRows())
	}

	if err := iter.Close(); err != nil {
		t.Fatalf(errorInClosingIter, err)
	}
}

func handleBatchExecution(t *testing.T, batch *gocql.Batch) {
	err := session.ExecuteBatch(batch)
	if err != nil {
		t.Errorf(errorWhileExecutingBatch, err.Error())
	}
}

func selectAndValidateMultipleData(t *testing.T, session *gocql.Session, userID string, expectedResults []struct {
	uploadTime int64
	commEvent  string
}) {
	query := "SELECT upload_time, event_data FROM keyspace1.event WHERE user_id = ? ORDER BY upload_time ASC;"
	iter := session.Query(query, userID).Iter()

	var uploadTime int64
	var commEvent []byte
	i := 0

	for iter.Scan(&uploadTime, &commEvent) {
		if i >= len(expectedResults) {
			t.Fatal(errorMoreRowsThanExpected)
		}
		if uploadTime != expectedResults[i].uploadTime || string(commEvent) != expectedResults[i].commEvent {
			t.Fatalf(unexpectedRow, i, uploadTime, commEvent, expectedResults[i].uploadTime, expectedResults[i].commEvent)
		}
		i++
	}
	if i != len(expectedResults) {
		t.Fatalf(errorExpectedRows, len(expectedResults), i)
	}

	if err := iter.Close(); err != nil {
		t.Fatalf(errorInClosingIter, err)
	}
}

// Integration test cases covering basic operation over all supported datatype
func TestIntegrationSupportForDataDiffDataTypes(t *testing.T) {
	const (
		insertQuery            = `INSERT INTO keyspace1.validate_data_types (text_col, blob_col, timestamp_col, int_col, bigint_col, float_col, bool_col, uuid_col, map_bool_col, map_str_col, map_ts_col, list_str_col, set_str_col) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
		updateQuery            = `UPDATE keyspace1.validate_data_types SET int_col = ?, float_col = ? WHERE text_col = ? `
		deleteQuery            = `DELETE FROM keyspace1.validate_data_types WHERE text_col = ?`
		selectQueryForDataType = `SELECT text_col, blob_col, timestamp_col,int_col, bigint_col, float_col, bool_col, uuid_col, map_bool_col, map_str_col, map_ts_col, list_str_col, set_str_col FROM keyspace1.validate_data_types WHERE text_col = ? AND uuid_col = ? ALLOW FILTERING`
	)
	timestamp, err := time.Parse(time.RFC3339, "2024-07-12T06:47:28.303Z")
	if err != nil {
		t.Fatalf("Failed to parse timestamp: %v", err)
	}
	args := map[string]interface{}{
		"text_col":      "sample_text",
		"blob_col":      []byte("sample_blob"),
		"timestamp_col": timestamp,
		"int_col":       123,
		"bigint_col":    int64(123456789),
		"float_col":     float32(1.23),
		"bool_col":      true,
		"uuid_col":      gocql.MustRandomUUID(),
		"map_bool_col":  map[string]bool{"key1": true},
		"map_str_col":   map[string]string{"key2": "value2"},
		"map_ts_col":    map[string]time.Time{"key3": timestamp},
		"list_str_col":  []string{"item1", "item2"},
		"set_str_col":   []string{"item1", "item2"},
	}
	tests := []struct {
		name        string
		runTest     func(t *testing.T)
		expectedErr error
	}{
		{
			name: "Insert data",
			runTest: func(t *testing.T) {

				id := gocql.MustRandomUUID()
				args["uuid_col"] = id
				err = session.Query(insertQuery, args["text_col"], args["blob_col"], args["timestamp_col"], args["int_col"], args["bigint_col"], args["float_col"], args["bool_col"], args["uuid_col"],
					args["map_bool_col"], args["map_str_col"], args["map_ts_col"], args["list_str_col"], args["set_str_col"]).Exec()
				if err != nil {
					t.Fatalf(errorInsertFailed, err)
				}
				validateData(t, session, selectQueryForDataType, args["text_col"].(string), id, args)
			},
		},
		{
			name: "Update data",
			runTest: func(t *testing.T) {
				id := gocql.MustRandomUUID()
				args["uuid_col"] = id
				err = session.Query(insertQuery, args["text_col"], args["blob_col"], args["timestamp_col"], args["int_col"], args["bigint_col"], args["float_col"], args["bool_col"], args["uuid_col"],
					args["map_bool_col"], args["map_str_col"], args["map_ts_col"], args["list_str_col"], args["set_str_col"]).Exec()
				if err != nil {
					t.Fatalf("%s - %v", errorWhileInsert, err)
				}
				args["int_col"] = 456
				args["float_col"] = float32(4.56)
				err = session.Query(updateQuery, args["int_col"], args["float_col"], args["text_col"].(string)).Exec()
				if err != nil {
					t.Fatalf("Update failed: %v", err)
				}
				validateData(t, session, selectQueryForDataType, args["text_col"].(string), id, args)
			},
		},
		{
			name: "Delete data",
			runTest: func(t *testing.T) {
				id := gocql.MustRandomUUID()
				args["uuid_col"] = id
				err = session.Query(insertQuery, args["text_col"], args["blob_col"], args["timestamp_col"], args["int_col"], args["bigint_col"], args["float_col"], args["bool_col"], args["uuid_col"],
					args["map_bool_col"], args["map_str_col"], args["map_ts_col"], args["list_str_col"], args["set_str_col"]).Exec()
				if err != nil {
					t.Fatalf(errorInsertFailed, err)
				}

				err = session.Query(deleteQuery, args["text_col"]).Exec()
				if err != nil {
					t.Fatalf("Delete failed: %v", err)
				}
				validateNoData(t, session, selectQueryForDataType, args["text_col"].(string), id)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.runTest(t)
		})
	}
}

// select and validate data of table - keyspace1.validate_data_types
func validateData(t *testing.T, session *gocql.Session, query, text string, id gocql.UUID, expectedData map[string]interface{}) {
	iter := session.Query(query, text, id).Iter()
	m := map[string]interface{}{}
	for iter.MapScan(m) {
		for k, v := range expectedData {
			actualValue, exists := m[k]
			if !exists {
				t.Fatalf(errorValidationFailed, k, v, nil)
			}

			switch expectedValue := v.(type) {
			case time.Time:
				actualTime, ok := actualValue.(time.Time)
				if !ok {
					t.Fatalf(errorValidationType, k, typeTime, actualValue)
				}
				if !expectedValue.UTC().Equal(actualTime.UTC()) {
					t.Fatalf(errorValidationFailed, k, expectedValue.UTC(), actualTime.UTC())
				}
			case float32:
				if actualValue != expectedValue {
					t.Fatalf(errorValidationFailed, k, expectedValue, actualValue)
				}
			case float64:
				if actualValue != expectedValue {
					t.Fatalf(errorValidationFailed, k, expectedValue, actualValue)
				}
			case []byte:
				actualBytes, ok := actualValue.([]byte)
				if !ok {
					t.Fatalf(errorValidationType, k, typeTime, actualValue)
				}
				if !reflect.DeepEqual(actualBytes, expectedValue) {
					t.Fatalf(errorValidationFailed, k, expectedValue, actualBytes)
				}
			case map[string]bool:
				actualMap, ok := actualValue.(map[string]bool)
				if !ok {
					t.Fatalf(errorValidationType, k, typeMapStringBool, actualValue)
				}
				if !reflect.DeepEqual(actualMap, expectedValue) {
					t.Fatalf(errorValidationFailed, k, expectedValue, actualMap)
				}
			case map[string]string:
				actualMap, ok := actualValue.(map[string]string)
				if !ok {
					t.Fatalf(errorValidationType, k, typeMapStringString, actualValue)
				}
				if !reflect.DeepEqual(actualMap, expectedValue) {
					t.Fatalf(errorValidationFailed, k, expectedValue, actualMap)
				}
			case map[string]time.Time:
				actualMap, ok := actualValue.(map[string]time.Time)
				if !ok {
					t.Fatalf(errorValidationType, k, typeMapStringTime, actualValue)
				}
				if !reflect.DeepEqual(actualMap, expectedValue) {
					t.Fatalf(errorValidationFailed, k, expectedValue, actualMap)
				}
			case []string:
				actualList, ok := actualValue.([]string)
				if !ok {
					t.Fatalf(errorValidationType, k, typeListOfString, actualValue)
				}
				if !reflect.DeepEqual(actualList, expectedValue) {
					t.Fatalf(errorValidationFailed, k, expectedValue, actualList)
				}
			default:
				if !reflect.DeepEqual(actualValue, v) {
					t.Fatalf(errorValidationFailed, k, v, actualValue)
				}
			}
		}
	}
	if err := iter.Close(); err != nil {
		t.Fatalf(errorQueryFailed, err)
	}
}

// select and validate if no row found for table - keyspace1.validate_data_types
func validateNoData(t *testing.T, session *gocql.Session, query string, text string, id gocql.UUID) {
	iter := session.Query(query, text, id).Iter()
	if iter.NumRows() > 0 {
		t.Errorf(errorExpectedRows, 0, iter.NumRows())
	}
	if err := iter.Close(); err != nil {
		t.Fatalf(errorQueryFailed, err)
	}
}
