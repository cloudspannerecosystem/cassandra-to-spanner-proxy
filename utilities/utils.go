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

package utilities

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/proxycore"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

const (
	arrayOfString = "array[string]"
	BigintType    = "bigint"
)

// Compile a regular expression that matches the WHERE clause with at least one space before and after it.
// The regex uses case insensitive matching and captures everything after the WHERE clause.
// \s+ matches one or more spaces before and after WHERE.
// (.+) captures everything after WHERE and its trailing spaces.
var whereRegex = regexp.MustCompile(`(?i)\s+WHERE\s+(.+)`)

var (
	MapTextCassandraType       = datatype.NewMapType(datatype.Varchar, datatype.Varchar)
	MapBigintCassandraType     = datatype.NewMapType(datatype.Varchar, datatype.Bigint)
	MapIntCassandraType        = datatype.NewMapType(datatype.Varchar, datatype.Int)
	MapBooleanCassandraType    = datatype.NewMapType(datatype.Varchar, datatype.Boolean)
	MapTimestampCassandraType  = datatype.NewMapType(datatype.Varchar, datatype.Timestamp)
	MapDoubleCassandraType     = datatype.NewMapType(datatype.Varchar, datatype.Double)
	MapDateCassandraType       = datatype.NewMapType(datatype.Varchar, datatype.Date)
	MapBlobCassandraType       = datatype.NewMapType(datatype.Varchar, datatype.Blob)
	ListTextCassandraType      = datatype.NewListType(datatype.Varchar)
	ListBigintCassandraType    = datatype.NewListType(datatype.Bigint)
	ListBooleanCassandraType   = datatype.NewListType(datatype.Boolean)
	ListIntCassandraType       = datatype.NewListType(datatype.Int)
	ListDoubleCassandraType    = datatype.NewListType(datatype.Double)
	ListFloatCassandraType     = datatype.NewListType(datatype.Float)
	ListTimestampCassandraType = datatype.NewListType(datatype.Timestamp)
	ListDateCassandraType      = datatype.NewListType(datatype.Date)
	ListBlobCassandraType      = datatype.NewListType(datatype.Blob)
	SetTextCassandraType       = datatype.NewSetType(datatype.Varchar)
	SetBigintCassandraType     = datatype.NewSetType(datatype.Bigint)
	SetBooleanCassandraType    = datatype.NewSetType(datatype.Boolean)
	SetIntCassandraType        = datatype.NewSetType(datatype.Int)
	SetDoubleCassandraType     = datatype.NewSetType(datatype.Double)
	SetFloatCassandraType      = datatype.NewSetType(datatype.Float)
	SetTimestampCassandraType  = datatype.NewSetType(datatype.Timestamp)
	SetDateCassandraType       = datatype.NewSetType(datatype.Date)
	SetBlobCassandraType       = datatype.NewSetType(datatype.Blob)
)

var (
	EncodedTrue, _  = proxycore.EncodeType(datatype.Boolean, primitive.ProtocolVersion4, true)
	EncodedFalse, _ = proxycore.EncodeType(datatype.Boolean, primitive.ProtocolVersion4, false)
)

const secondsThreshold = int64(1e10)
const millisecondsThreshold = int64(1e13)
const microsecondsThreshold = int64(1e16)
const nanosecondsThreshold = int64(1e18)

type ExecuteOptions struct {
	MaxStaleness int64
}

// FlattenTableName formats the tableName based on the keyspace naming requirement.
//
// The function checks if the keyspace name should be prefixed to the table name
// based on the keyspaceFlatter boolean flag. If true, the keyspace name is prefixed
// to the table name separated by an underscore. The resulting table name is then
// converted to lowercase to ensure consistency in naming conventions.
//
// Parameters:
// - keyspaceFlatter: A boolean that determines whether to prefix the keyspace name to the table name.
// - keyspace: The name of the keyspace to potentially prepend.
// - tableName: The original table name.
//
// Returns:
// - string: The possibly modified table name which is always returned in lowercase.
//
// Example Usage:
// - FlattenTableName(true, "UserKeyspace", "UserInfo") returns "userkeyspace_userinfo"
// - FlattenTableName(false, "UserKeyspace", "UserInfo") returns "userinfo"

func FlattenTableName(keyspaceFlatter bool, keyspace string, tableName string) string {
	if keyspaceFlatter {
		tableName = fmt.Sprintf("%s_%s", keyspace, tableName)
	}
	return strings.ToLower(tableName)
}

// formatTimestamp formats the Timestamp into time.
func FormatTimestamp(ts int64) (*time.Time, error) {
	if ts == 0 {
		return nil, errors.New("no valid timestamp found")
	}
	var formattedValue time.Time

	switch {
	case ts < secondsThreshold:
		formattedValue = time.Unix(ts, 0)
	case ts >= secondsThreshold && ts < millisecondsThreshold:
		formattedValue = time.UnixMilli(ts)
	case ts >= millisecondsThreshold && ts < microsecondsThreshold:
		formattedValue = time.UnixMicro(ts)
	case ts >= microsecondsThreshold && ts < nanosecondsThreshold:
		seconds := ts / int64(time.Second)
		// Get the remaining nanoseconds
		nanoseconds := ts % int64(time.Second)
		formattedValue = time.Unix(seconds, nanoseconds)
	default:
		return nil, errors.New("no valid timestamp found")
	}

	return &formattedValue, nil
}

// GetSpannerColumnType takes a string representing a Cassandra column data type and
// converts it into the corresponding Google Cloud Spanner column data type.
//
// Parameters:
//   - c: A string representing the Cassandra column data type. This function is designed
//     to accept common Cassandra data types and their variations, such as "text",
//     "blob", "timestamp", various "map" and "list" types, etc.
//
// Returns:
//   - A string representing the equivalent Google Cloud Spanner column data type. For
//     example, Cassandra's "text" type is converted to Spanner's "string" type, and
//     "bigint" is converted to "int64".
func GetSpannerColumnType(cqlType string) string {
	switch cqlType {
	case "text":
		return "string"
	case "timestamp":
		return "timestamp"
	case "blob":
		return "bytes"
	case "bigint":
		return "int64"
	case "int", "int64":
		return "int64"
	case "boolean":
		return "bool"
	case "list<text>", "set<text>", "frozen<list<text>>", "frozen<set<text>>":
		return arrayOfString
	case "map<text, boolean>":
		return "array[string, bool]"
	case "float", "double":
		return "float64"
	case "uuid":
		return "string"
	case "map<text, text>":
		return "array[string, string]"
	case "map<text, timestamp>":
		return "array[string, timestamp]"
	// Future scope - Below datatypes are currently not supported. To be added in future.
	// case "map<text, int>":
	// 	return "string"
	// case "map<text, float64>":
	// 	return "string"
	// case "map<text, date>":
	// 	return "string"
	// case "list<text, text>":
	// 	return "string"
	default:
		return ""
	}
}

// getCassandraColumnType converts a string representation of a Cassandra data type into
// a corresponding DataType value. It supports a range of common Cassandra data types,
// including text, blob, timestamp, int, bigint, boolean, uuid, various map and list types.
//
// Parameters:
//   - c: A string representing the Cassandra column data type. This function expects
//     the data type in a specific format (e.g., "text", "int", "map<text, boolean>").
//
// Returns:
//   - datatype.DataType: The corresponding DataType value for the provided string.
//     This is used to represent the Cassandra data type in a structured format within Go.
//   - error: An error is returned if the provided string does not match any of the known
//     Cassandra data types. This helps in identifying unsupported or incorrectly specified
//     data types.
func GetCassandraColumnType(choice string) (datatype.DataType, error) {
	switch choice {
	case "map<text, boolean>":
		return MapBooleanCassandraType, nil
	case "list<text>", "frozen<list<text>>":
		return ListTextCassandraType, nil
	case "set<text>", "frozen<set<text>>":
		return SetTextCassandraType, nil
	case "text":
		return datatype.Varchar, nil
	case "timestamp":
		return datatype.Timestamp, nil
	case "blob":
		return datatype.Blob, nil
	case "bigint":
		return datatype.Bigint, nil
	case "int":
		return datatype.Int, nil
	case "boolean":
		return datatype.Boolean, nil
	case "double":
		return datatype.Double, nil
	case "float":
		return datatype.Float, nil
	case "map<text, bigint>":
		return MapBigintCassandraType, nil
	case "map<text, text>":
		return MapTextCassandraType, nil
	case "map<text, timestamp>":
		return MapTimestampCassandraType, nil
	case "map<text, int>":
		return MapIntCassandraType, nil
	case "map<text, double>":
		return MapDoubleCassandraType, nil
	case "map<text, date>":
		return MapDateCassandraType, nil
	case "list<boolean>", "frozen<list<boolean>>":
		return ListBooleanCassandraType, nil
	case "list<int>", "frozen<list<int>>":
		return ListIntCassandraType, nil
	case "list<bigint>", "frozen<list<bigint>>":
		return ListBigintCassandraType, nil
	case "list<double>", "frozen<list<double>>":
		return ListDoubleCassandraType, nil
	case "list<date>", "frozen<list<date>>":
		return ListDateCassandraType, nil
	case "list<timestamp>", "frozen<list<timestamp>>":
		return ListTimestampCassandraType, nil
	case "set<boolean>", "frozen<set<boolean>>":
		return SetBooleanCassandraType, nil
	case "set<int>", "frozen<set<int>>":
		return SetIntCassandraType, nil
	case "set<bigint>", "frozen<set<bigint>>":
		return SetBigintCassandraType, nil
	case "set<double>", "frozen<set<double>>":
		return SetDoubleCassandraType, nil
	case "set<date>", "frozen<set<date>>":
		return SetDateCassandraType, nil
	case "set<timestamp>", "frozen<set<timestamp>>":
		return SetTimestampCassandraType, nil
	case "uuid":
		return datatype.Uuid, nil
	default:
		return nil, fmt.Errorf("%s", "Error in returning column type")
	}
}

// TypeConversion converts a Go data type to a Cassandra protocol-compliant byte array.
//
// Parameters:
//   - s: The data to be converted.
//   - protocalV: Cassandra protocol version.
//
// Returns: Byte array in Cassandra protocol format.
func TypeConversion(s interface{}, protocalV primitive.ProtocolVersion) ([]byte, error) {
	var bytes []byte
	var err error
	switch v := s.(type) {
	case string:
		bytes, err = proxycore.EncodeType(datatype.Varchar, protocalV, v)
	case time.Time:
		bytes, err = proxycore.EncodeType(datatype.Timestamp, protocalV, v)
	case []byte:
		bytes, err = proxycore.EncodeType(datatype.Blob, protocalV, v)
	case int64:
		bytes, err = proxycore.EncodeType(datatype.Bigint, protocalV, v)
	case int:
		bytes, err = proxycore.EncodeType(datatype.Int, protocalV, v)
	case bool:
		if v {
			return EncodedTrue, nil
		} else {
			return EncodedFalse, nil
		}
	case map[string]string:
		bytes, err = proxycore.EncodeType(MapTextCassandraType, protocalV, v)
	case float64:
		bytes, err = proxycore.EncodeType(datatype.Double, protocalV, v)
	case float32:
		bytes, err = proxycore.EncodeType(datatype.Float, protocalV, v)
	case []string:
		bytes, err = proxycore.EncodeType(SetTextCassandraType, protocalV, v)
	case spanner.NullDate:
		var date time.Time = time.Date(v.Date.Year, v.Date.Month, v.Date.Day, 0, 0, 0, 0, time.UTC)
		bytes, err = proxycore.EncodeType(datatype.Date, protocalV, date)
	default:
		err = fmt.Errorf("%v - %v", "Unknown Datatype Identified", s)
	}

	return bytes, err
}

// parseDurationToSeconds converts a duration string like "1s", "1m", "1h" to the equivalent number of seconds in int64.
func ParseDurationToSeconds(duration string) (int64, error) {
	if len(duration) < 2 {
		return 0, fmt.Errorf("invalid duration format")
	}

	// Extract the numeric part and the unit
	value := duration[:len(duration)-1]
	unit := duration[len(duration)-1:]

	// Convert the numeric part to an integer
	num, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid number in duration: %v", err)
	}

	// Check for negative values
	if num < 0 {
		return 0, fmt.Errorf("negative values are not allowed in duration: %s", duration)
	}

	// Convert to seconds based on the unit
	switch unit {
	case "s": // seconds
		return int64(num), nil
	case "m": // minutes
		return int64(num) * int64(time.Minute.Seconds()), nil
	case "h": // hours
		return int64(num) * int64(time.Hour.Seconds()), nil
	default:
		return 0, fmt.Errorf("invalid duration unit: %s", unit)
	}
}
