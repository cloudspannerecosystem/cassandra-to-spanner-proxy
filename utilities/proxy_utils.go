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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/proxycore"
	customencoder "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/zap"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/credentials"
)

const (
	keyValueEncoding = "key-value"
	consoleEncoding  = "console"
	defaultEncoding  = "json"
)

type LoggerConfig struct {
	OutputType string `yaml:"outputType"`
	Filename   string `yaml:"fileName"`
	MaxSize    int    `yaml:"maxSize"`    // megabytes
	MaxBackups int    `yaml:"maxBackups"` // The value of MaxBackups determines how many previous log files are kept after a new log file is created due to the MaxSize or MaxAge limits.
	MaxAge     int    `yaml:"maxAge"`     // days
	Compress   bool   `yaml:"compress"`   // the rotated log files to be compressed to save disk space.
	Encoding   string `yaml:"encoding"`
}

// DecodeBytesToSpannerColumnType - Function to decode incoming bytes parameter
// for handleExecute scenario into corresponding go datatype
//
// Parameters:
//   - b: []byte
//   - choice:  datatype.DataType
//   - protocolVersion: primitive.ProtocolVersion
//
// Returns: (interface{}, error)
func DecodeBytesToSpannerColumnType(b []byte, choice datatype.DataType, protocolVersion primitive.ProtocolVersion) (interface{}, error) {

	switch choice.GetDataTypeCode() {
	case primitive.DataTypeCodeVarchar:
		return proxycore.DecodeType(datatype.Varchar, protocolVersion, b)
	case primitive.DataTypeCodeTimestamp:
		return proxycore.DecodeType(datatype.Timestamp, protocolVersion, b)
	case primitive.DataTypeCodeBlob:
		return proxycore.DecodeType(datatype.Blob, protocolVersion, b)
	case primitive.DataTypeCodeBigint:
		return proxycore.DecodeType(datatype.Bigint, protocolVersion, b)
	case primitive.DataTypeCodeInt:
		bytes, err := proxycore.DecodeType(datatype.Int, protocolVersion, b)
		// casting result to int64 as int32 not supported by spanner
		if err != nil {
			return nil, err
		}
		if res, ok := bytes.(int32); ok {
			return int64(res), err
		} else {
			return nil, fmt.Errorf("error while casting to int64")
		}
	case primitive.DataTypeCodeBoolean:
		if b[0] == 0 {
			return false, nil
		} else {
			return true, nil
		}
	case primitive.DataTypeCodeList:
		return handleList(b, choice, protocolVersion)
	case primitive.DataTypeCodeMap:
		return handleMap(b, choice, protocolVersion)
	case primitive.DataTypeCodeSet:
		return handleSet(b, choice, protocolVersion)
	case primitive.DataTypeCodeDouble:
		return proxycore.DecodeType(datatype.Double, protocolVersion, b)
	case primitive.DataTypeCodeFloat:
		bytes, err := proxycore.DecodeType(datatype.Float, protocolVersion, b)
		// casting result to float64 as float32 not supported by spanner
		if err != nil {
			return nil, err
		}
		if res, ok := bytes.(float32); ok {
			float32Str := fmt.Sprintf("%f", res)
			// Convert string to float64
			f64, err := strconv.ParseFloat(float32Str, 64)
			if err != nil {
				return nil, fmt.Errorf("error converting string to float64: %s", err)
			}
			return f64, err
		} else {
			return nil, fmt.Errorf("error while casting to float64")
		}
	case primitive.DataTypeCodeUuid:
		decodedUuid, err := proxycore.DecodeType(datatype.Uuid, protocolVersion, b)
		if err != nil {
			return nil, err
		}
		if primitiveUuid, ok := decodedUuid.(primitive.UUID); ok {
			return primitiveUuid.String(), err
		}
		return nil, fmt.Errorf("unable to decode uuid")
	case primitive.DataTypeCodeDate:
		return proxycore.DecodeType(datatype.Date, protocolVersion, b)

	}
	return nil, fmt.Errorf("unhandled datatype for decoder - %s", choice)
}

func handleList(b []byte, choice datatype.DataType, protocolVersion primitive.ProtocolVersion) (interface{}, error) {
	var err error
	switch choice.String() {
	case "list<varchar>":
		return proxycore.DecodeType(ListTextCassandraType, protocolVersion, b)
	case "list<bigint>":
		return proxycore.DecodeType(ListBigintCassandraType, protocolVersion, b)
	case "list<int>":
		// casting result into array of int64 as int32 not supported by spanner
		listP, err := proxycore.DecodeType(ListIntCassandraType, protocolVersion, b)
		var result []int64
		if pointerList, ok := listP.([]*int32); ok {
			for _, v := range pointerList {
				result = append(result, int64(*v))
			}
			return result, err
		} else {
			return nil, fmt.Errorf("error while decoding list of int32")
		}
	case "list<boolean>":
		return proxycore.DecodeType(ListBooleanCassandraType, protocolVersion, b)
	case "list<timestamp>":
		return proxycore.DecodeType(ListTimestampCassandraType, protocolVersion, b)
	case "list<blob>":
		return proxycore.DecodeType(ListBlobCassandraType, protocolVersion, b)
	case "list<date>":
		return proxycore.DecodeType(ListDateCassandraType, protocolVersion, b)
	case "list<double>":
		return proxycore.DecodeType(ListDoubleCassandraType, protocolVersion, b)
	default:
		err = fmt.Errorf("unsupported Datatype to decode - %s", choice)
		return nil, err
	}
}
func handleSet(b []byte, choice datatype.DataType, protocolVersion primitive.ProtocolVersion) (interface{}, error) {
	var err error
	switch choice.String() {
	case "set<varchar>":
		return proxycore.DecodeType(SetTextCassandraType, protocolVersion, b)
	case "set<boolean>":
		return proxycore.DecodeType(SetBooleanCassandraType, protocolVersion, b)
	case "set<int>":
		listP, err := proxycore.DecodeType(SetIntCassandraType, protocolVersion, b)
		// casting result into array of int64 as int32 not supported by spanner
		var result []int64
		if pointerList, ok := listP.([]*int32); ok {
			for _, v := range pointerList {
				result = append(result, int64(*v))
			}
			return result, err
		} else {
			return nil, fmt.Errorf("error while decoding list of int32")
		}
	case "set<bigint>":
		return proxycore.DecodeType(SetBigintCassandraType, protocolVersion, b)
	case "set<blob>":
		return proxycore.DecodeType(SetBlobCassandraType, protocolVersion, b)
	case "set<date>":
		return proxycore.DecodeType(SetDateCassandraType, protocolVersion, b)
	case "set<timestamp>":
		return proxycore.DecodeType(SetTimestampCassandraType, protocolVersion, b)
	case "set<double>":
		return proxycore.DecodeType(SetDoubleCassandraType, protocolVersion, b)
	default:
		err = fmt.Errorf("unsupported Datatype to decode - %s", choice)
		return nil, err
	}
}

func handleMap(b []byte, choice datatype.DataType, protocolVersion primitive.ProtocolVersion) (interface{}, error) {
	switch choice.String() {
	case "map<varchar,boolean>":
		decodedValue, err := proxycore.DecodeType(MapBooleanCassandraType, protocolVersion, b)
		if err != nil {
			return nil, err
		}
		decodedMap := make(map[string]bool)
		if pointerMap, ok := decodedValue.(map[*string]*bool); ok {
			for k, v := range pointerMap {
				decodedMap[*k] = *v
			}
		}
		return spanner.NullJSON{Value: decodedMap, Valid: true}, nil
	case "map<varchar,varchar>":
		decodedValue, err := proxycore.DecodeType(MapTextCassandraType, protocolVersion, b)
		if err != nil {
			return nil, err
		}
		decodedMap := make(map[string]string)
		if pointerMap, ok := decodedValue.(map[*string]*string); ok {
			for k, v := range pointerMap {
				decodedMap[*k] = *v
			}
		}
		return spanner.NullJSON{Value: decodedMap, Valid: true}, nil
	case "map<varchar,int>":
		decodedValue, err := proxycore.DecodeType(MapIntCassandraType, protocolVersion, b)
		if err != nil {
			return nil, err
		}
		decodedMap := make(map[string]int64)
		if pointerMap, ok := decodedValue.(map[*string]*int32); ok {
			for k, v := range pointerMap {
				decodedMap[*k] = int64(*v)
			}
		}
		return spanner.NullJSON{Value: decodedMap, Valid: true}, nil
	case "map<varchar,bigint>":
		decodedValue, err := proxycore.DecodeType(MapBigintCassandraType, protocolVersion, b)
		if err != nil {
			return nil, err
		}
		decodedMap := make(map[string]int64)
		if pointerMap, ok := decodedValue.(map[*string]*int64); ok {
			for k, v := range pointerMap {
				decodedMap[*k] = *v
			}
		}
		return spanner.NullJSON{Value: decodedMap, Valid: true}, nil
	case "map<varchar,blob>":
		decodedValue, err := proxycore.DecodeType(MapBlobCassandraType, protocolVersion, b)
		if err != nil {
			return nil, err
		}
		decodedMap := make(map[string][]byte)
		if pointerMap, ok := decodedValue.(map[*string][]byte); ok {
			for k, v := range pointerMap {
				decodedMap[*k] = v
			}
		}
		return spanner.NullJSON{Value: decodedMap, Valid: true}, nil
	case "map<varchar,date>":
		decodedValue, err := proxycore.DecodeType(MapDateCassandraType, protocolVersion, b)
		if err != nil {
			return nil, err
		}
		decodedMap := make(map[string]time.Time)
		if pointerMap, ok := decodedValue.(map[*string]*time.Time); ok {
			for k, v := range pointerMap {
				decodedMap[*k] = *v
			}
		}
		return spanner.NullJSON{Value: decodedMap, Valid: true}, nil
	case "map<varchar,timestamp>":
		decodedValue, err := proxycore.DecodeType(MapTimestampCassandraType, protocolVersion, b)
		if err != nil {
			return nil, err
		}
		decodedMap := make(map[string]time.Time)
		if pointerMap, ok := decodedValue.(map[*string]*time.Time); ok {
			for k, v := range pointerMap {
				decodedMap[*k] = *v
			}
		}
		return spanner.NullJSON{Value: decodedMap, Valid: true}, nil
	case "map<varchar,double>":
		decodedValue, err := proxycore.DecodeType(MapDoubleCassandraType, protocolVersion, b)
		if err != nil {
			return nil, err
		}
		decodedMap := make(map[string]float64)
		if pointerMap, ok := decodedValue.(map[*string]*float64); ok {
			for k, v := range pointerMap {
				decodedMap[*k] = *v
			}
		}
		return spanner.NullJSON{Value: decodedMap, Valid: true}, nil
	default:
		return nil, fmt.Errorf("unsupported Datatype to decode - %s", choice)
	}
}

// keyExists checks if a key is present in a list of strings.
func KeyExistsInList(key string, list []string) bool {
	for _, item := range list {
		if item == key {
			return true // Key found
		}
	}
	return false // Key not found
}

// AddSecondsToCurrentTimestamp takes a number of seconds as input
// and returns the current Unix timestamp plus the input time in seconds.
func AddSecondsToCurrentTimestamp(seconds int64) string {
	// Get the current time
	currentTime := time.Now()

	// Add the input seconds to the current time
	futureTime := currentTime.Add(time.Second * time.Duration(seconds))

	// Return the future time as a Unix timestamp (in seconds)
	return unixToISO(futureTime.Unix())
}

// unixToISO converts a Unix timestamp (in seconds) to an ISO 8601 formatted string.
func unixToISO(unixTimestamp int64) string {
	// Convert the Unix timestamp to a time.Time object
	t := time.Unix(unixTimestamp, 0).UTC()

	// Format the time as an ISO 8601 string
	return t.Format(time.RFC3339)
}

// ExtractAfterWhere ensures there is at least one space before and after the WHERE clause before splitting.
func ExtractAfterWhere(sqlQuery string) (string, error) {
	// Find the first match and return the captured group.
	matches := whereRegex.FindStringSubmatch(sqlQuery)
	if len(matches) < 2 {
		return "", fmt.Errorf("no WHERE clause found or it doesn't have spaces correctly placed around it")
	}

	// Return the portion of the query after the WHERE clause.
	// matches[1] contains the captured group which is everything after the WHERE clause and its preceding space(s).
	return matches[1], nil
}

// SetupLogger initializes a zap.Logger instance based on the provided log level and logger configuration.
// If loggerConfig specifies file output, it sets up a file-based logger. Otherwise, it defaults to console output.
// Returns the configured zap.Logger or an error if setup fails.
func SetupLogger(logLevel string, loggerConfig *LoggerConfig) (*zap.Logger, error) {
	level := getLogLevel(logLevel)

	if loggerConfig != nil && loggerConfig.OutputType == "file" {
		return setupFileLogger(level, loggerConfig)
	}

	encoding := defaultEncoding
	if loggerConfig != nil {
		encoding = defaultIfEmpty(loggerConfig.Encoding, defaultEncoding)
	}

	return setupConsoleLogger(level, encoding)
}

// getLogLevel translates a string log level to a zap.AtomicLevel.
// Supports "info", "debug", "error", and "warn" levels, defaulting to "info" if an unrecognized level is provided.
func getLogLevel(logLevel string) zap.AtomicLevel {
	level := zap.NewAtomicLevel()

	switch logLevel {
	case "info":
		level.SetLevel(zap.InfoLevel)
	case "debug":
		level.SetLevel(zap.DebugLevel)
	case "error":
		level.SetLevel(zap.ErrorLevel)
	case "warn":
		level.SetLevel(zap.WarnLevel)
	default:
		level.SetLevel(zap.InfoLevel)
	}

	return level
}

// setupFileLogger configures a zap.Logger for file output using a lumberjack.Logger for log rotation.
// Accepts a zap.AtomicLevel and a LoggerConfig struct to customize log output and rotation settings.
// Returns the configured zap.Logger or an error if setup fails.
func setupFileLogger(level zap.AtomicLevel, loggerConfig *LoggerConfig) (*zap.Logger, error) {
	rotationalLogger := &lumberjack.Logger{
		Filename:   defaultIfEmpty(loggerConfig.Filename, "/var/log/cassandra-to-spanner-proxy/output.log"),
		MaxSize:    loggerConfig.MaxSize,                       // megabytes, default 100MB
		MaxAge:     defaultIfZero(loggerConfig.MaxAge, 3),      // setting default value to 3 days
		MaxBackups: defaultIfZero(loggerConfig.MaxBackups, 10), // setting default max backups to 10 files
		Compress:   loggerConfig.Compress,
	}

	cfg := zap.NewProductionEncoderConfig()
	var encoder zapcore.Encoder
	switch loggerConfig.Encoding {
	case keyValueEncoding:
		encoder = customencoder.NewKeyValueEncoder(cfg)
	case consoleEncoding:
		encoder = zapcore.NewConsoleEncoder(cfg)
	default:
		encoder = zapcore.NewJSONEncoder(cfg)
	}

	core := zapcore.NewCore(
		encoder,
		zapcore.AddSync(rotationalLogger),
		level,
	)

	return zap.New(core), nil
}

// setupConsoleLogger configures a zap.Logger for console output.
// Accepts a zap.AtomicLevel to set the logging level.
// Returns the configured zap.Logger or an error if setup fails.
func setupConsoleLogger(level zap.AtomicLevel, encoding string) (*zap.Logger, error) {
	if encoding == keyValueEncoding {
		registerKeyValueEncoder()
	}
	config := zap.Config{
		Encoding:         encoding,
		Level:            level, // default log level
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "time",
			CallerKey:      "caller",
			LevelKey:       "level",
			NameKey:        "logger",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder, // or zapcore.LowercaseColorLevelEncoder for console
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
	}

	return config.Build()
}

func registerKeyValueEncoder() {
	zap.RegisterEncoder(keyValueEncoding, func(c zapcore.EncoderConfig) (zapcore.Encoder, error) {
		return customencoder.NewKeyValueEncoder(c), nil
	})
}

// defaultIfEmpty returns a default string value if the provided value is empty.
// Useful for setting default configuration values.
func defaultIfEmpty(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}

// defaultIfZero returns a default integer value if the provided value is zero.
// Useful for setting default configuration values.
func defaultIfZero(value, defaultValue int) int {
	if value == 0 {
		return defaultValue
	}
	return value
}

// Get SystemSchema Raw Cached Values
func getSystemSchemaRawCachedValues(hdr *frame.Header, s [][]interface{}) ([]message.Row, error) {
	var data = make([][][]byte, len(s))
	for i, row := range s {
		dataRow := make([][]byte, len(row))
		for j, val := range row {
			bytes, err := TypeConversion(val, hdr.Version)
			if err != nil {
				return nil, err
			}
			dataRow[j] = bytes
		}
		data[i] = dataRow
	}
	return data, nil
}

// creates credentials for establishing TLS/mTLS connection to external spanner host
func NewCred(ca_certificate, client_certificate, client_key string) (credentials.TransportCredentials, error) {
	if ca_certificate == "" {
		return nil, fmt.Errorf("ca_certificate is required to establish TLS/mTLS connection")
	}
	caCert, err := os.ReadFile(ca_certificate)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate file: %w", err)
	}

	capool := x509.NewCertPool()
	if !capool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append the CA certificate to CA pool")
	}
	if client_certificate == "" && client_key == "" {
		return credentials.NewTLS(&tls.Config{RootCAs: capool}), nil
	}
	if client_certificate == "" || client_key == "" {
		return nil, fmt.Errorf("Both client_certificate and client_key to establish mTLS connection")
	}
	cert, err := tls.LoadX509KeyPair(client_certificate, client_key)
	if err != nil {
		return nil, fmt.Errorf("failed to load client cert and key: %w", err)
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: capool}), nil
}
