// Copyright (c) DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

const (
	CountValueName = "count(*)"
)

var systemTables = []string{"local", "peers", "peers_v2", "schema_keyspaces", "schema_columnfamilies", "schema_columns", "schema_usertypes", "tables", "keyspaces", "columns", "functions", "triggers", "indexes", "views", "types", "aggregates"}

var nonIdempotentFuncs = []string{"uuid", "now"}

type ValueLookupFunc func(name string) (value message.Column, err error)

func FilterValues(stmt *SelectStatement, columns []*message.ColumnMetadata, valueFunc ValueLookupFunc) (filtered []message.Column, err error) {
	if _, ok := stmt.Selectors[0].(*StarSelector); ok {
		for _, column := range columns {
			var val message.Column
			val, err = valueFunc(column.Name)
			if err != nil {
				return nil, err
			}
			filtered = append(filtered, val)
		}
	} else {
		for _, selector := range stmt.Selectors {
			var val message.Column
			val, err = valueFromSelector(selector, valueFunc)
			if err != nil {
				return nil, err
			}
			filtered = append(filtered, val)
		}
	}
	return filtered, nil
}

func valueFromSelector(selector Selector, valueFunc ValueLookupFunc) (val message.Column, err error) {
	switch s := selector.(type) {
	case *CountStarSelector:
		return valueFunc(CountValueName)
	case *IDSelector:
		return valueFunc(s.Name)
	case *AliasSelector:
		return valueFromSelector(s.Selector, valueFunc)
	default:
		return nil, errors.New("unhandled selector type")
	}
}

func FilterColumns(stmt *SelectStatement, columns []*message.ColumnMetadata) (filtered []*message.ColumnMetadata, err error) {
	if _, ok := stmt.Selectors[0].(*StarSelector); ok {
		filtered = columns
	} else {
		for _, selector := range stmt.Selectors {
			var column *message.ColumnMetadata
			column, err = columnFromSelector(selector, columns, stmt.Keyspace, stmt.Table)
			if err != nil {
				return nil, err
			}
			filtered = append(filtered, column)
		}
	}
	return filtered, nil
}

func isCountSelector(selector Selector) bool {
	_, ok := selector.(*CountStarSelector)
	return ok
}

func IsCountStarQuery(stmt *SelectStatement) bool {
	if len(stmt.Selectors) == 1 {
		if isCountSelector(stmt.Selectors[0]) {
			return true
		} else if alias, ok := stmt.Selectors[0].(*AliasSelector); ok {
			return isCountSelector(alias.Selector)
		}
	}
	return false
}

func columnFromSelector(selector Selector, columns []*message.ColumnMetadata, keyspace string, table string) (column *message.ColumnMetadata, err error) {
	switch s := selector.(type) {
	case *CountStarSelector:
		return &message.ColumnMetadata{
			Keyspace: keyspace,
			Table:    table,
			Name:     s.Name,
			Type:     datatype.Int,
		}, nil
	case *IDSelector:
		if column = FindColumnMetadata(columns, s.Name); column != nil {
			return column, nil
		} else {
			return nil, fmt.Errorf("invalid column %s", s.Name)
		}
	case *AliasSelector:
		column, err = columnFromSelector(s.Selector, columns, keyspace, table)
		if err != nil {
			return nil, err
		}
		alias := *column // Make a copy so we can modify the name
		alias.Name = s.Alias
		return &alias, nil
	default:
		return nil, errors.New("unhandled selector type")
	}
}

func isSystemTable(name Identifier) bool {
	for _, table := range systemTables {
		if name.equal(table) {
			return true
		}
	}
	return false
}

func isNonIdempotentFunc(name Identifier) bool {
	for _, funcName := range nonIdempotentFuncs {
		if name.equal(funcName) {
			return true
		}
	}
	return false
}

func isUnreservedKeyword(l *lexer, t token, keyword string) bool {
	return tkIdentifier == t && l.identifier().equal(keyword)
}

func skipToken(l *lexer, t token, toSkip token) token {
	if t == toSkip {
		return l.next()
	}
	return t
}

func untilToken(l *lexer, to token) token {
	var t token
	for to != t && tkEOF != t {
		t = l.next()
	}
	return t
}

func parseQualifiedIdentifier(l *lexer) (keyspace, target Identifier, t token, err error) {
	temp := l.identifier()
	if t = l.next(); tkDot == t {
		if t = l.next(); tkIdentifier != t {
			return Identifier{}, Identifier{}, tkInvalid, errors.New("expected another identifier after '.' for qualified identifier")
		}
		return temp, l.identifier(), l.next(), nil
	} else {
		return Identifier{}, temp, t, nil
	}
}

func parseIdentifiers(l *lexer, t token) (err error) {
	for tkRparen != t && tkEOF != t {
		if tkIdentifier != t {
			return errors.New("expected identifier")
		}
		t = skipToken(l, l.next(), tkComma)
	}
	if tkRparen != t {
		return errors.New("expected closing ')' for identifiers")
	}
	return nil
}

func isDMLTerminator(t token) bool {
	return t == tkEOF || t == tkEOS || t == tkInsert || t == tkUpdate || t == tkDelete || t == tkApply
}

func IsQueryHandledWithQueryType(keyspace Identifier, query string) (handled bool, stmt Statement, queryType string, err error) {
	var l lexer
	l.init(query)

	t := l.next()
	switch t {
	case tkSelect:
		handled, stmt, err := isHandledSelectStmt(&l, keyspace)
		return handled, stmt, "select", err
	case tkInsert:
		return false, nil, "insert", nil
	case tkUpdate:
		return false, nil, "update", nil
	case tkDelete:
		return false, nil, "delete", nil
	case tkUse:
		handled, stmt, err := isHandledUseStmt(&l)
		return handled, stmt, "", err
	}
	return false, nil, "", nil

}

// IsQueryCreateTableType checks whether the given CQL query string is a CREATE TABLE query type.
// It converts the query to lowercase to ensure it matches tokens case-insensitively before parsing.
//
// Parameters:
// - query: A string representing the CQL query to be analyzed.
//
// Returns:
// - A boolean value: true if the query is a "CREATE TABLE" type, false otherwise.
func IsQueryCreateTableType(query string) bool {
	// Convert the query to lowercase to handle CQL keywords regardless of their case.
	lowerQuery := strings.ToLower(query)

	// Initialize a CQL-specific lexer with the lowercase query string.
	// This lexer will tokenize the query for further analysis.
	var l lexer
	l.init(lowerQuery)

	// Retrieve the first token from the lexer, which should represent the first keyword in the query.
	t := l.next()

	// Check if the first token is the 'CREATE' keyword.
	switch t {
	case tkCreate:
		// If 'CREATE' is found, return true, indicating the query could be creating a table.
		// Note: This does not verify subsequent tokens (e.g., 'TABLE') are present.
		return true
	default:
		// If any other token is found as the first token, return false, indicating the query is not for table creation.
		return false
	}
}
