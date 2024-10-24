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
	"os"
	"strings"
	"testing"
)

func TestCheckGCPCredentials(t *testing.T) {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/path/to/credentials.json")
	defer os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS")

	t.Run("Credentials Set", func(t *testing.T) {
		if err := checkGCPCredentials(); err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("Credentials Not Set", func(t *testing.T) {
		os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS")
		if err := checkGCPCredentials(); err == nil {
			t.Errorf("Expected error, got nil")
		}
	})
}

func TestExtractQueries(t *testing.T) {
	content := "CREATE TABLE test (id INT PRIMARY KEY);\n-- This is a comment\nCREATE TABLE another_test (id INT PRIMARY KEY);\n"
	filepath := "test.cql"

	// Write the content to a temporary file
	if err := os.WriteFile(filepath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}
	defer os.Remove(filepath)

	expectedQueries := []string{
		"CREATE TABLE test (id INT PRIMARY KEY); ",
		"CREATE TABLE another_test (id INT PRIMARY KEY); ",
	}

	t.Run("Extract Queries Successfully", func(t *testing.T) {
		queries, err := extractQueries(filepath)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		for i, expected := range expectedQueries {
			if strings.TrimSpace(queries[i]) != strings.TrimSpace(expected) {
				t.Errorf("Expected %v, got %v", expected, queries[i])
			}
		}
	})
}

func TestGetPrimaryKeyInfo(t *testing.T) {
	primaryKeys := []string{"id", "name", "email"}

	tests := []struct {
		columnName         string
		expectedIsPK       bool
		expectedPrecedence int
	}{
		{"id", true, 1},
		{"name", true, 2},
		{"email", true, 3},
		{"address", false, 0},
	}

	for _, test := range tests {
		t.Run(test.columnName, func(t *testing.T) {
			isPrimary, precedence := getPrimaryKeyInfo(test.columnName, primaryKeys)
			if isPrimary != test.expectedIsPK || precedence != test.expectedPrecedence {
				t.Errorf("For column %v, expected %v, %v, got %v, %v",
					test.columnName, test.expectedIsPK, test.expectedPrecedence, isPrimary, precedence)
			}
		})
	}
}
