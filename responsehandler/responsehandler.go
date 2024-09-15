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

package responsehandler

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ollionorg/cassandra-to-spanner-proxy/translator"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/ollionorg/cassandra-to-spanner-proxy/tableConfig"
	"github.com/ollionorg/cassandra-to-spanner-proxy/third_party/datastax/proxycore"
	"github.com/ollionorg/cassandra-to-spanner-proxy/utilities"
	"go.uber.org/zap"
)

const (
	spannerTTLColumn = "spanner_ttl_ts"
	spannerTSColumn  = "last_commit_ts"
	bigintType       = "bigint"
	intType          = "int"
	textType         = "text"
	uuidType         = "uuid"
	floatType        = "float"
	doubleType       = "double"
	timestampType    = "timestamp"
)

var (
	unixEpoch   = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultDate = ConvertToCQLDate(unixEpoch)
)

type QueryMetadata struct {
	Query                string
	QueryType            string
	TableName            string
	KeyspaceName         string
	ProtocalV            primitive.ProtocolVersion
	Params               map[string]interface{}
	SelectedColumns      []tableConfig.SelectedColumns
	Paramkeys            []string
	ParamValues          []interface{}
	UsingTSCheck         string
	HasIfNotExists       bool
	SelectQueryForDelete string
	PrimaryKeys          []string
	SelectQueryMapUpdate string // select Query for map update by key Scenario
	UpdateSetValues      []translator.UpdateSetValue
	MutationKeyRange     []interface{}
	AliasMap             map[string]tableConfig.AsKeywordMeta
	ExecuteOptions       utilities.ExecuteOptions
}

// Only used for unit testing
type JsonStringGetter interface {
	GetJsonString(i int, row *spanner.Row) (string, error)
}

type TypeHandler struct {
	Logger      *zap.Logger
	TableConfig *tableConfig.TableConfig
	JsonStringGetter
	ProtocalV primitive.ProtocolVersion
}

// GetColumnMeta retrieves the metadata for a column based on the provided query metadata.
// Parameters:
//   - columnName: Name of the column to get metadata for.
//   - query: QueryMetadata containing details about the query.
//
// Returns: Column metadata as a string and an error if any.
func (th *TypeHandler) GetColumnMeta(columnName string, tableName string) (string, error) {
	metaStr, err := th.TableConfig.GetColumnType(tableName, columnName)
	if err != nil {
		return "", err
	}
	return metaStr.CQLType, nil
}

// TypeConversionForWriteTime converts a time.Time value to a byte slice for write operations,
// encoding it into a format compatible with the specified protocol version.
//
// Parameters:
// - s: The time.Time value to be converted.
//
// Returns:
// - []byte: The encoded byte slice representing the time value.
// - error: Any error encountered during the encoding process.
func (th *TypeHandler) TypeConversionForWriteTime(s time.Time) ([]byte, error) {
	var bytes []byte
	var err error
	timeInMicrosecond := s.UnixMicro()
	bytes, err = proxycore.EncodeType(datatype.Bigint, th.ProtocalV, timeInMicrosecond)
	if err != nil {
		th.Logger.Error("Error while Encoding Timestamp -> ", zap.Error(err))
	}
	return bytes, err
}

// function to encode rows - [][]interface{} to cassandra supported response formate [][][]bytes
func (th *TypeHandler) BuildResponseForSystemQueries(rows [][]interface{}, protocalV primitive.ProtocolVersion) ([]message.Row, error) {
	var allRows []message.Row
	for _, row := range rows {
		var mr message.Row
		for _, val := range row {
			encodedByte, err := utilities.TypeConversion(val, protocalV)
			if err != nil {
				return allRows, err
			}
			mr = append(mr, encodedByte)
		}
		allRows = append(allRows, mr)
	}
	return allRows, nil
}

// BuildColumnMetadata constructs metadata for columns based on the given Spanner row type and query metadata.
// It returns a slice of functions to process each column and a slice of column metadata.
//
// Parameters:
// - rowType: The Spanner row type containing field definitions.
// - protocalV: primitive.ProtocolVersion
// - aliasMap: map[string]translator.AsKeywordMeta
// - tableName string
// - keyspace string
// Returns:
// - A slice of functions to process each column.
// - A slice of column metadata.
// - An error if any issues arise during processing.
func (th *TypeHandler) BuildColumnMetadata(rowType *spannerpb.StructType,
	protocalV primitive.ProtocolVersion,
	aliasMap map[string]tableConfig.AsKeywordMeta,
	tableName, keyspace string) ([]func(int, *spanner.Row) ([]byte, error), []*message.ColumnMetadata, error) {

	var (
		rowFuncs []func(int, *spanner.Row) ([]byte, error)
		cmd      []*message.ColumnMetadata
		dt       datatype.DataType
	)
	th.ProtocalV = protocalV

	for i, field := range rowType.GetFields() {
		if field.Name == spannerTTLColumn || field.Name == spannerTSColumn {
			continue
		}

		cqlType, err := th.getCqlType(field.Name, aliasMap, tableName)
		if err != nil {
			return nil, cmd, err
		}
		var rowFunc func(int, *spanner.Row) ([]byte, error)

		switch field.Type.Code {
		case spannerpb.TypeCode_STRING:
			switch cqlType {
			case textType:
				dt = datatype.Varchar
				rowFunc = th.HandleCassandraTextType
			case uuidType:
				dt = datatype.Uuid
				rowFunc = th.HandleCassandraUuidType
			default:
				return nil, nil, fmt.Errorf("invalid Varchar type - %s", cqlType)
			}
		case spannerpb.TypeCode_TIMESTAMP:
			switch cqlType {
			case timestampType:
				dt = datatype.Timestamp
				rowFunc = th.HandleCassandraTimestampType
			case bigintType:
				dt = datatype.Bigint
				rowFunc = th.HandleCassandraWriteTimeFuncType
			default:
				return nil, nil, fmt.Errorf("unknown cassandra type for spanner timestamp - %s", cqlType)
			}
		case spannerpb.TypeCode_BYTES:
			dt = datatype.Blob
			rowFunc = th.HandleCassandraBlobType
		case spannerpb.TypeCode_INT64:
			switch cqlType {
			case bigintType:
				dt = datatype.Bigint
				rowFunc = th.HandleCassandraBigintType
			case intType:
				dt = datatype.Int
				rowFunc = th.HandleCassandraIntType
			default:
				return nil, nil, fmt.Errorf("invalid int64 type - %s", cqlType)
			}
		case spannerpb.TypeCode_BOOL:
			dt = datatype.Boolean
			rowFunc = th.HandleCassandraBoolType
		case spannerpb.TypeCode_ARRAY:
			dt, err = utilities.GetCassandraColumnType(cqlType)
			if err != nil {
				return nil, cmd, err
			}
			rowFunc, err = th.HandleArrayType(dt)
			if err != nil {
				return nil, cmd, err
			}
		case spannerpb.TypeCode_JSON:
			dt, err = utilities.GetCassandraColumnType(cqlType)
			if err != nil {
				return nil, cmd, err
			}
			rowFunc, err = th.HandleMapType(dt)
			if err != nil {
				return nil, cmd, err
			}
		case spannerpb.TypeCode_FLOAT64:
			switch cqlType {
			case floatType:
				dt = datatype.Float
				rowFunc = th.HandleCassandraFloatType
			case doubleType:
				dt = datatype.Double
				rowFunc = th.HandleCassandraDoubleType
			default:
				return nil, cmd, fmt.Errorf("unable to find appropriate float type")
			}
		case spannerpb.TypeCode_DATE:
			dt = datatype.Date
			rowFunc = th.HandleCassandraDateType
		default:
			return nil, cmd, fmt.Errorf("spanner type is not handled - %v", field.Type.Code)
		}

		rowFuncs = append(rowFuncs, rowFunc)
		cmd = append(cmd, &message.ColumnMetadata{
			Keyspace: keyspace,
			Table:    tableName,
			Name:     field.Name,
			Index:    int32(i),
			Type:     dt,
		})
	}

	return rowFuncs, cmd, nil
}

// BuildResponseRow constructs a response row by applying a series of functions to each column of a Spanner row.
// It uses the provided row functions to encode each column and constructs a message row.
//
// Parameters:
// - row: The Spanner row to be processed.
// - rowFuncs: A slice of functions to process each column in the row.
// Returns:
// - A message.Row, which is a slice of byte slices representing the encoded columns.
// - An error if any issues arise during processing.
func (th *TypeHandler) BuildResponseRow(row *spanner.Row, rowFuncs []func(int, *spanner.Row) ([]byte, error)) (message.Row, error) {
	var mr message.Row
	for i, rowFunc := range rowFuncs {
		byteData, err := rowFunc(i, row)
		if err != nil {
			return nil, fmt.Errorf("error while encoding data - %v, %d, %v ", err, i, row)
		}
		mr = append(mr, byteData)
	}
	return mr, nil
}

// handle encoding spanner data to cassandra bool type
func (th *TypeHandler) HandleCassandraBoolType(i int, row *spanner.Row) ([]byte, error) {
	var err error
	var col spanner.NullBool
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Boolean data: %v", err)
	}
	if col.Valid {
		if col.Bool {
			return utilities.EncodedTrue, nil
		} else {
			return utilities.EncodedFalse, nil
		}
	}
	return nil, err
}

// handle encoding spanner data to cassandra float type
func (th *TypeHandler) HandleCassandraFloatType(i int, row *spanner.Row) ([]byte, error) {
	var err error
	var col spanner.NullFloat64
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Float64 data: %v", err)
	}
	if col.Valid {
		return proxycore.EncodeType(datatype.Float, th.ProtocalV, float32(col.Float64))
	}
	return nil, err

}

// handle encoding spanner data to cassandra Double type
func (th *TypeHandler) HandleCassandraDoubleType(i int, row *spanner.Row) ([]byte, error) {
	var err error
	var col spanner.NullFloat64
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Float64 data: %v", err)
	}
	if col.Valid {
		return proxycore.EncodeType(datatype.Double, th.ProtocalV, col.Float64)
	}
	return nil, err

}

// handle encoding spanner data to cassandra blob type
func (th *TypeHandler) HandleCassandraBlobType(i int, row *spanner.Row) ([]byte, error) {
	var col []byte
	var err error
	if err = row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Bytes data: %v", err)
	}
	if col != nil {
		return proxycore.EncodeType(datatype.Blob, th.ProtocalV, col)
	}
	return nil, err
}

// handle encoding spanner data to cassandra date type
func (th *TypeHandler) HandleCassandraDateType(i int, row *spanner.Row) ([]byte, error) {
	var col spanner.NullDate
	var err error
	if err = row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Date data: %v", err)
	}
	if col.Valid {
		date := time.Date(col.Date.Year, col.Date.Month, col.Date.Day, 0, 0, 0, 0, time.UTC)
		daysSinceEpoch := ConvertToCQLDate(date)
		return proxycore.EncodeType(datatype.Date, th.ProtocalV, daysSinceEpoch)
	}
	return nil, err
}

// handle encoding spanner data to cassandra timestamp type
func (th *TypeHandler) HandleCassandraTimestampType(i int, row *spanner.Row) ([]byte, error) {
	var col spanner.NullTime
	var err error
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Timestamp data: %v", err)
	}
	if col.Valid {
		return proxycore.EncodeType(datatype.Timestamp, th.ProtocalV, col.Time)
	}
	return nil, err
}

// handle encoding spanner timestamp to cassandra Bigint type
func (th *TypeHandler) HandleCassandraWriteTimeFuncType(i int, row *spanner.Row) ([]byte, error) {
	var err error
	var byteData []byte = nil
	var col spanner.NullTime
	if err = row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Timestamp data: %v", err)
	}
	if col.Valid {
		byteData, err = th.TypeConversionForWriteTime(col.Time)
	}
	return byteData, err
}

// handle encoding spanner data to cassandra text type
func (th *TypeHandler) HandleCassandraTextType(i int, row *spanner.Row) ([]byte, error) {
	var err error
	var col spanner.NullString
	if err = row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve STRING data: %v", err)
	}
	if col.Valid {
		return proxycore.EncodeType(datatype.Varchar, th.ProtocalV, col.StringVal)
	}
	return nil, err
}

// handle encoding spanner data to cassandra Uuid type
func (th *TypeHandler) HandleCassandraUuidType(i int, row *spanner.Row) ([]byte, error) {
	var err error
	var col spanner.NullString
	if err = row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve STRING data: %v", err)
	}
	if col.Valid {
		return proxycore.EncodeType(datatype.Uuid, th.ProtocalV, col.StringVal)
	}
	return nil, err
}

// handle encoding spanner data to cassandra Bigint type
func (th *TypeHandler) HandleCassandraBigintType(i int, row *spanner.Row) ([]byte, error) {
	var err error
	var col spanner.NullInt64
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve INT64 data: %v", err)
	}
	if col.Valid {
		return proxycore.EncodeType(datatype.Bigint, th.ProtocalV, col.Int64)
	}
	return nil, err
}

// handle encoding spanner data to cassandra Int type
func (th *TypeHandler) HandleCassandraIntType(i int, row *spanner.Row) ([]byte, error) {
	var err error
	var col spanner.NullInt64
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Int data: %v", err)
	}
	if col.Valid {
		return proxycore.EncodeType(datatype.Int, th.ProtocalV, int32(col.Int64))
	}
	return nil, err
}

// getCqlType retrieves the CQL type for a given field name based on the query metadata.
//
// Parameters:
// - fieldName: The name of the field for which the CQL type is to be retrieved.
// - aliasMap: map containing alias metadata.
// - tableName: string
// Returns:
// - string: The CQL type of the specified field.
// - error: An error object if the CQL type cannot be determined.
func (th *TypeHandler) getCqlType(fieldName string, aliasMap map[string]tableConfig.AsKeywordMeta, tableName string) (string, error) {
	if meta, found := aliasMap[fieldName]; found {
		return meta.CQLType, nil
	}
	return th.GetColumnMeta(fieldName, tableName)
}

func (th *TypeHandler) HandleMapType(dt datatype.DataType) (func(int, *spanner.Row) ([]byte, error), error) {
	switch dt {
	case utilities.MapTextCassandraType:
		return th.HandleMapStringString, nil
	case utilities.MapBooleanCassandraType:
		return th.HandleMapStringBool, nil
	case utilities.MapTimestampCassandraType:
		return th.HandleMapStringTimestamp, nil
	case utilities.MapBigintCassandraType:
		return th.HandleMapStringInt64, nil
	case utilities.MapDoubleCassandraType:
		return th.HandleMapStringFloat64, nil
	case utilities.MapDateCassandraType:
		return th.HandleMapStringDate, nil
	default:
		return nil, fmt.Errorf("unsupported MAP element type: %v", dt)
	}
}

func (th *TypeHandler) GetJsonString(i int, row *spanner.Row) (string, error) {
	var col spanner.NullJSON
	if err := row.Column(i, &col); err != nil {
		return "", fmt.Errorf("failed to retrieve JSON data: %v", err)
	}
	return col.String(), nil
}

// handle encoding spanner data to cassandra map<varchar, bigint> type
func (th *TypeHandler) HandleMapStringInt64(i int, row *spanner.Row) ([]byte, error) {
	var detailsField map[string]int64
	jsonStr, err := th.GetJsonString(i, row)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal([]byte(jsonStr), &detailsField); err != nil {
		return nil, fmt.Errorf("error deserializing JSON to map[string]int64: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.MapBigintCassandraType, th.ProtocalV, detailsField)
	return bytes, err
}

// handle encoding spanner data to cassandra map<varchar, bool> type
func (th *TypeHandler) HandleMapStringBool(i int, row *spanner.Row) ([]byte, error) {
	var detailsField map[string]bool
	jsonStr, err := th.GetJsonString(i, row)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(jsonStr), &detailsField); err != nil {
		return nil, fmt.Errorf("error deserializing JSON to map[string]bool: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.MapBooleanCassandraType, th.ProtocalV, detailsField)
	return bytes, err
}

// handle encoding spanner data to cassandra map<varchar, varchar> type
func (th *TypeHandler) HandleMapStringString(i int, row *spanner.Row) ([]byte, error) {
	var detailsField map[string]string
	jsonStr, err := th.GetJsonString(i, row)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(jsonStr), &detailsField); err != nil {
		return nil, fmt.Errorf("error deserializing JSON to map[string]string: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.MapTextCassandraType, th.ProtocalV, detailsField)
	return bytes, err
}

// handle encoding spanner data to cassandra map<varchar, double> type
func (th *TypeHandler) HandleMapStringFloat64(i int, row *spanner.Row) ([]byte, error) {
	var detailsField map[string]float64
	jsonStr, err := th.GetJsonString(i, row)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(jsonStr), &detailsField); err != nil {
		return nil, fmt.Errorf("error deserializing JSON to map[string]float64: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.MapDoubleCassandraType, th.ProtocalV, detailsField)
	return bytes, err
}

// handle encoding spanner data to cassandra map<varchar, date> type
func (th *TypeHandler) HandleMapStringDate(i int, row *spanner.Row) ([]byte, error) {
	var col map[string]spanner.NullDate
	jsonStr, err := th.GetJsonString(i, row)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(jsonStr), &col); err != nil {
		return nil, fmt.Errorf("error deserializing JSON to map[string]date: %v", err)
	}

	convertedMap := make(map[string]int32)
	for key, nullDate := range col {
		if nullDate.Valid {
			date := time.Date(nullDate.Date.Year, nullDate.Date.Month, nullDate.Date.Day, 0, 0, 0, 0, time.UTC)
			daysSinceEpoch := ConvertToCQLDate(date)
			convertedMap[key] = daysSinceEpoch
		}
	}

	bytes, err := proxycore.EncodeType(utilities.MapDateCassandraType, th.ProtocalV, convertedMap)
	return bytes, err
}

// handle encoding spanner data to cassandra map<varchar, timestamp> type
func (th *TypeHandler) HandleMapStringTimestamp(i int, row *spanner.Row) ([]byte, error) {
	var detailsField map[string]time.Time
	jsonStr, err := th.GetJsonString(i, row)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(jsonStr), &detailsField); err != nil {
		return nil, fmt.Errorf("error deserializing JSON to map[string]time.Time: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.MapTimestampCassandraType, th.ProtocalV, detailsField)
	return bytes, err
}

func (th *TypeHandler) HandleArrayType(dt datatype.PrimitiveType) (func(int, *spanner.Row) ([]byte, error), error) {
	switch dt {
	case utilities.ListTextCassandraType:
		return th.HandleStringArrayType, nil
	case utilities.SetTextCassandraType:
		return th.HandleStringSetType, nil
	case utilities.ListTimestampCassandraType:
		return th.HandleTimestampArrayType, nil
	case utilities.ListBlobCassandraType:
		return th.HandleByteArrayType, nil
	case utilities.ListBigintCassandraType:
		return th.HandleInt64ArrayType, nil
	case utilities.ListIntCassandraType:
		return th.HandleInt32ArrayType, nil
	case utilities.ListBooleanCassandraType:
		return th.HandleBoolArrayType, nil
	case utilities.ListDoubleCassandraType:
		return th.HandleFloat64ArrayType, nil
	case utilities.ListFloatCassandraType:
		return th.HandleFloatArrayType, nil
	case utilities.ListDateCassandraType:
		return th.HandleDateArrayType, nil
	case utilities.SetBigintCassandraType:
		return th.HandleInt64SetType, nil
	case utilities.SetIntCassandraType:
		return th.HandleInt32SetType, nil
	case utilities.SetBooleanCassandraType:
		return th.HandleBoolSetType, nil
	case utilities.SetDoubleCassandraType:
		return th.HandleFloat64SetType, nil
	case utilities.SetFloatCassandraType:
		return th.HandleFloatSetType, nil
	case utilities.SetDateCassandraType:
		return th.HandleDateSetType, nil
	case utilities.SetTimestampCassandraType:
		return th.HandleTimestampSetType, nil
	case utilities.SetBlobCassandraType:
		return th.HandleByteSetType, nil
	default:
		return nil, fmt.Errorf("unsupported element type: %v", dt)
	}
}

// The following functions (HandleInt64ArrayType, HandleStringArrayType, handle**ArrayType etc.) follow a similar pattern:
// They handle the conversion of specific array types from Spanner to Cassandra.
// Parameters are similar, involving the row reference, column index.
// Each returns a Cassandra datatype, the converted bytes, and an error if any.
func (th *TypeHandler) HandleInt64ArrayType(i int, row *spanner.Row) ([]byte, error) {
	var col []int64
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve ARRAY<Int64> data: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.ListBigintCassandraType, th.ProtocalV, col)
	return bytes, err
}

func (th *TypeHandler) HandleInt64SetType(i int, row *spanner.Row) ([]byte, error) {
	var col []int64
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Set<Int64> data: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.SetBigintCassandraType, th.ProtocalV, col)
	return bytes, err
}

func (th *TypeHandler) HandleInt32ArrayType(i int, row *spanner.Row) ([]byte, error) {
	var col []int64
	var col32 []int32
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve ARRAY<Int32> data: %v", err)
	}

	for _, val := range col {
		col32 = append(col32, int32(val))
	}

	bytes, err := proxycore.EncodeType(utilities.ListIntCassandraType, th.ProtocalV, col32)
	return bytes, err
}

func (th *TypeHandler) HandleInt32SetType(i int, row *spanner.Row) ([]byte, error) {
	var col []int64
	var col32 []int32
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Set<Int32> data: %v", err)
	}

	for _, val := range col {
		col32 = append(col32, int32(val))
	}

	bytes, err := proxycore.EncodeType(utilities.SetIntCassandraType, th.ProtocalV, col32)
	return bytes, err
}

func (th *TypeHandler) HandleStringArrayType(i int, row *spanner.Row) ([]byte, error) {
	var col []string
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve ARRAY<STRING> data: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.ListTextCassandraType, th.ProtocalV, col)
	return bytes, err
}

func (th *TypeHandler) HandleStringSetType(i int, row *spanner.Row) ([]byte, error) {
	var col []string
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Set<STRING> data: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.SetTextCassandraType, th.ProtocalV, col)
	return bytes, err
}

func (th *TypeHandler) HandleBoolArrayType(i int, row *spanner.Row) ([]byte, error) {
	var col []bool
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve ARRAY<BOOL> data: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.ListBooleanCassandraType, th.ProtocalV, col)
	return bytes, err
}

func (th *TypeHandler) HandleBoolSetType(i int, row *spanner.Row) ([]byte, error) {
	var col []bool
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Set<BOOL> data: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.SetBooleanCassandraType, th.ProtocalV, col)
	return bytes, err
}

func (th *TypeHandler) HandleFloat64ArrayType(i int, row *spanner.Row) ([]byte, error) {
	var col []float64
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve ARRAY<float64> data: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.ListDoubleCassandraType, th.ProtocalV, col)
	return bytes, err
}

func (th *TypeHandler) HandleFloat64SetType(i int, row *spanner.Row) ([]byte, error) {
	var col []float64
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Set<float64> data: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.SetDoubleCassandraType, th.ProtocalV, col)
	return bytes, err
}

func (th *TypeHandler) HandleFloatArrayType(i int, row *spanner.Row) ([]byte, error) {
	var col []float64
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve ARRAY<float64> data: %v", err)
	}
	var col32 []float32
	for _, val := range col {
		col32 = append(col32, float32(val))
	}
	bytes, err := proxycore.EncodeType(utilities.ListFloatCassandraType, th.ProtocalV, col32)
	return bytes, err
}

func (th *TypeHandler) HandleFloatSetType(i int, row *spanner.Row) ([]byte, error) {
	var col []float64
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Set<float64> data: %v", err)
	}
	var col32 []float32
	for _, val := range col {
		col32 = append(col32, float32(val))
	}
	bytes, err := proxycore.EncodeType(utilities.SetFloatCassandraType, th.ProtocalV, col32)
	return bytes, err
}

func (th *TypeHandler) HandleDateArrayType(i int, row *spanner.Row) ([]byte, error) {
	var col []spanner.NullDate
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve ARRAY<date> data: %v", err)
	}
	var cqlDates []int32
	for _, nullDate := range col {
		if nullDate.Valid {
			t := ConvertCivilDateToTime(nullDate.Date)
			cqlDate := ConvertToCQLDate(t)
			cqlDates = append(cqlDates, cqlDate)
		} else {
			cqlDates = append(cqlDates, defaultDate)
		}
	}
	bytes, err := proxycore.EncodeType(utilities.ListDateCassandraType, th.ProtocalV, cqlDates)
	return bytes, err
}

func (th *TypeHandler) HandleDateSetType(i int, row *spanner.Row) ([]byte, error) {
	var col []spanner.NullDate
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Set<date> data: %v", err)
	}
	var cqlDates []int32
	for _, nullDate := range col {
		if nullDate.Valid {
			t := ConvertCivilDateToTime(nullDate.Date)
			cqlDate := ConvertToCQLDate(t)
			cqlDates = append(cqlDates, cqlDate)
		} else {
			cqlDates = append(cqlDates, defaultDate)
		}
	}
	bytes, err := proxycore.EncodeType(utilities.SetDateCassandraType, th.ProtocalV, cqlDates)
	return bytes, err
}

func (th *TypeHandler) HandleTimestampArrayType(i int, row *spanner.Row) ([]byte, error) {
	var col []time.Time
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve ARRAY<Timestamp> data: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.ListTimestampCassandraType, th.ProtocalV, col)
	return bytes, err
}

func (th *TypeHandler) HandleTimestampSetType(i int, row *spanner.Row) ([]byte, error) {
	var col []time.Time
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Set<Timestamp> data: %v", err)
	}
	bytes, err := proxycore.EncodeType(utilities.SetTimestampCassandraType, th.ProtocalV, col)
	return bytes, err
}

func (th *TypeHandler) HandleByteArrayType(i int, row *spanner.Row) ([]byte, error) {
	var col [][]byte
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve ARRAY<byte> data: %v", err)
	}
	var stringSlice []string
	for _, b := range col {
		stringSlice = append(stringSlice, string(b))
	}
	bytes, err := proxycore.EncodeType(utilities.ListBlobCassandraType, th.ProtocalV, stringSlice)
	return bytes, err
}

func (th *TypeHandler) HandleByteSetType(i int, row *spanner.Row) ([]byte, error) {
	var col [][]byte
	if err := row.Column(i, &col); err != nil {
		return nil, fmt.Errorf("failed to retrieve Set<byte> data: %v", err)
	}
	var stringSlice []string
	for _, b := range col {
		stringSlice = append(stringSlice, string(b))
	}
	bytes, err := proxycore.EncodeType(utilities.SetBlobCassandraType, th.ProtocalV, stringSlice)
	return bytes, err
}

// ConvertCivilDateToTime converts a civil.Date to a time.Time set to midnight UTC.
func ConvertCivilDateToTime(cDate civil.Date) time.Time {
	return time.Date(cDate.Year, time.Month(cDate.Month), cDate.Day, 0, 0, 0, 0, time.UTC)
}

// ConvertToCQLDate converts a time.Time to a CQL date as an int32 representing days since the Unix epoch.
func ConvertToCQLDate(t time.Time) int32 {
	return int32(t.Sub(unixEpoch).Hours() / 24)
}
