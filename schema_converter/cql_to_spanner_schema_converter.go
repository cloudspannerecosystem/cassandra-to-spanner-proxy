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
	"flag"
	"fmt"
	"log"
	"time"

	"bufio"
	"context"
	"os"
	"strings"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/parser"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/translator"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/utilities"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ColumnMetadata struct {
	ColumnName   string
	CQLType      string
	IsPrimary    bool
	PKPrecedence int
}

// checkGCPCredentials checks if Google Cloud credentials are set in the environment.
func checkGCPCredentials() error {
	credentials := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if credentials == "" {
		return fmt.Errorf("GCP credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS environment variable")
	}
	return nil
}

// extractQueries reads a CQL file and extracts queries line by line.
// It treats any line ending with a semicolon as a complete query.
func extractQueries(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var queries []string
	var currentQuery strings.Builder
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "//") {
			continue
		}

		currentQuery.WriteString(line + " ")

		// If the line ends with a semicolon, treat it as a full query
		if strings.HasSuffix(line, ";") {
			queries = append(queries, currentQuery.String())
			currentQuery.Reset()
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return queries, nil
}

// getPrimaryKeyInfo checks if a column is a primary key and returns the primary key precedence if found
func getPrimaryKeyInfo(columnName string, primaryKeys []string) (bool, int) {
	for i, pk := range primaryKeys {
		if columnName == pk {
			return true, i + 1
		}
	}
	return false, 0
}

// insertDataToTable inserts the mutations (rows) into the specified Spanner table
func insertDataToTable(ctx context.Context, client *spanner.Client, mutations []*spanner.Mutation) error {
	_, err := client.Apply(ctx, mutations)
	if err != nil {
		return fmt.Errorf("failed to insert data: %v", err)
	}
	return nil
}

// createSpannerTable sends a DDL query to Spanner to create or update a table
func createSpannerTable(ctx context.Context, queries []string, dbAdminClient *database.DatabaseAdminClient, db string) error {
	// Set a timeout for the DDL operation
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	req := &databasepb.UpdateDatabaseDdlRequest{
		Database:   db,
		Statements: queries,
	}

	// Initiate the DDL operation
	op, err := dbAdminClient.UpdateDatabaseDdl(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to initiate DDL operation: %v", err)
	}

	// Wait for the DDL operation to complete
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("failed to complete DDL operation: %v", err)
	}

	fmt.Println("DDL queries executed successfully")
	return nil
}

func main() {
	// Define command-line flags for required input parameters
	projectID := flag.String("project", "", "The project ID")
	instanceID := flag.String("instance", "", "The Spanner instance ID")
	databaseID := flag.String("database", "", "The Spanner database ID")
	endpoint := flag.String("endpoint", "", "The Spanner External Host")
	cqlFile := flag.String("cql", "", "Path to the CQL file")
	keyspaceFlatter := flag.Bool("keyspaceFlatter", false, "Whether to enable keyspace flattening (default: false)")
	tableName := flag.String("table", "TableConfigurations", "The name of the table (default: TableConfigurations)")
	enableUsingTimestamp := flag.Bool("enableUsingTimestamp", false, "Whether to enable using timestamp (default: false)")
	enableUsingTTL := flag.Bool("enableUsingTTL", false, "Whether to enable TTL (default: false)")
	usePlainText := flag.Bool("usePlainText", false, "Whether to use plain text to establish connection")
	ca_certificate := flag.String("ca_certificate", "", "The CA certificate file to use for TLS")
	client_certificate := flag.String("client_certificate", "", "The client certificate to establish mTLS for external hosts")
	client_key := flag.String("client_key", "", "The client key to establish mTLS for external hosts")

	flag.Parse()

	// Check if all required flags are provided
	checkMissingFlags := func() []string {
		missingFlags := []string{}

		if *projectID == "" {
			missingFlags = append(missingFlags, "-project")
		}
		if *instanceID == "" {
			missingFlags = append(missingFlags, "-instance")
		}
		if *databaseID == "" {
			missingFlags = append(missingFlags, "-database")
		}
		if *cqlFile == "" {
			missingFlags = append(missingFlags, "-cql")
		}
		return missingFlags
	}

	// Check for missing flags
	if missingFlags := checkMissingFlags(); len(missingFlags) > 0 {
		fmt.Println("Missing required flags:", missingFlags)
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Ensure that GCP credentials are set except for spanner external host connections
	if *endpoint == "" {
		if err := checkGCPCredentials(); err != nil {
			log.Fatalf("Error: %v", err)
		}
	}

	ctx := context.Background()

	// Construct the Spanner database path
	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", *projectID, *instanceID, *databaseID)

	// Create a Spanner Database Admin client
	var adminClient *database.DatabaseAdminClient
	var err error

	if *endpoint == "" {
		adminClient, err = database.NewDatabaseAdminClient(ctx)
	} else {
		if *usePlainText {
			adminClient, err = database.NewDatabaseAdminClient(
				ctx,
				option.WithEndpoint(*endpoint),
				option.WithoutAuthentication(),
				option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			)
		} else {
			creds, credsErr := utilities.NewCred(*ca_certificate, *client_certificate, *client_key)
			if credsErr != nil {
				log.Fatalf("%v", credsErr)
			}
			adminClient, err = database.NewDatabaseAdminClient(
				ctx,
				option.WithEndpoint(*endpoint),
				option.WithGRPCDialOption(grpc.WithTransportCredentials(creds)),
			)
		}
	}
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	// Define the query to create the TableConfigurations table
	createTableQuery := fmt.Sprintf(`
	  CREATE TABLE IF NOT EXISTS %s (
		KeySpaceName STRING(MAX),
		TableName STRING(MAX),
		ColumnName STRING(MAX),
		ColumnType STRING(MAX),
		IsPrimaryKey BOOL,
		PK_Precedence INT64
	  ) PRIMARY KEY(TableName, ColumnName, KeySpaceName)
	  `, *tableName)

	// Send the DDL query to create the table in Spanner
	req := &databasepb.UpdateDatabaseDdlRequest{
		Database:   db,
		Statements: []string{createTableQuery},
	}

	op, err := adminClient.UpdateDatabaseDdl(ctx, req)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Wait for the table creation to complete
	if err := op.Wait(ctx); err != nil {
		log.Fatalf("Failed to finish creating table: %v", err)
	}

	// Create a Spanner client to interact with the database
	var spannerClient *spanner.Client
	if *endpoint == "" {
		spannerClient, err = spanner.NewClient(ctx, db)
	} else {
		if *usePlainText {
			spannerClient, err = spanner.NewClient(ctx, db,
				option.WithEndpoint(*endpoint),
				option.WithoutAuthentication(),
				option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			)
		} else {
			creds, credsErr := utilities.NewCred(*ca_certificate, *client_certificate, *client_key)
			if credsErr != nil {
				log.Fatalf("%v", credsErr)
			}
			spannerClient, err = spanner.NewClient(ctx, db,
				option.WithEndpoint(*endpoint),
				option.WithGRPCDialOption(grpc.WithTransportCredentials(creds)),
			)
		}

	}
	if err != nil {
		log.Fatalf("Failed to create spanner client: %v", err)
	}
	defer spannerClient.Close()

	// Extract all queries from the provided CQL file
	queries, err := extractQueries(*cqlFile)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}

	// Create a new Translator object
	translatorObj := translator.Translator{}

	var mutations []*spanner.Mutation
	var spannerCreateTableQueries []string
	// Process each query from the CQL file
	for _, query := range queries {
		// Check if the query is a CREATE TABLE query
		if parser.IsQueryCreateTableType(query) && strings.Contains(strings.ToLower(query), "create table") {
			// Convert the CQL query to Spanner DDL using the Translator
			updateQueryMetadata, err := translatorObj.ToSpannerCreate(query, *keyspaceFlatter, *enableUsingTimestamp, *enableUsingTTL)
			if err != nil {
				fmt.Printf("Error parsing query: %v\n", err)
				return
			}

			// Ensure the Keyspace is present in the metadata
			if updateQueryMetadata.Keyspace == "" {
				fmt.Printf("Error: Keyspace missing for table %s\n", updateQueryMetadata.Table)
				continue
			}

			// Prepare Spanner mutations to insert metadata into the TableConfigurations table
			for _, column := range updateQueryMetadata.Columns {
				if column.CQLType != "" {
					isPrimary, pkPrecedence := getPrimaryKeyInfo(column.Name, updateQueryMetadata.PrimaryKeys)
					mutation := spanner.InsertOrUpdate(
						*tableName,
						[]string{"KeySpaceName", "TableName", "ColumnName", "ColumnType", "IsPrimaryKey", "PK_Precedence"},
						[]interface{}{updateQueryMetadata.Keyspace, updateQueryMetadata.Table, column.Name, column.CQLType, isPrimary, pkPrecedence},
					)
					mutations = append(mutations, mutation)
				}
			}
			spannerCreateTableQueries = append(spannerCreateTableQueries, updateQueryMetadata.SpannerQuery)
		}

	}
	// Insert metadata into the TableConfigurations table
	if err := insertDataToTable(ctx, spannerClient, mutations); err != nil {
		fmt.Printf("Failed to insert data: %v\n", err)
	}
	// Execute the DDL query to create the corresponding Spanner table
	if err := createSpannerTable(ctx, spannerCreateTableQueries, adminClient, db); err != nil {
		fmt.Printf("Error executing create table query: %v\n", err)
	}
}
