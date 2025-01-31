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
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/proxycore"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/tj/assert"
	"google.golang.org/grpc/credentials"
)

func jsonEqual(a, b string) bool {
	var obj1, obj2 interface{}
	if err := json.Unmarshal([]byte(a), &obj1); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(b), &obj2); err != nil {
		return false
	}
	return reflect.DeepEqual(obj1, obj2)
}

func truncateTimeToMillis(t time.Time) time.Time {
	return t.Truncate(time.Millisecond)
}

func TestDecodeBytesToSpannerColumnType(t *testing.T) {
	protocalV := primitive.ProtocolVersion4
	tm, _ := proxycore.EncodeType(datatype.Timestamp, protocalV, time.Unix(1234567890, 0))

	boolBytes, _ := proxycore.EncodeType(datatype.Boolean, protocalV, true)

	dateValue := time.Now().Truncate(24 * time.Hour).UTC()
	dateBytes, _ := proxycore.EncodeType(datatype.Date, protocalV, dateValue)

	blobValue := []byte("example blob data")
	blobBytes, _ := proxycore.EncodeType(datatype.Blob, protocalV, blobValue)

	//Map Type
	mapVarcharToBoolean := map[string]bool{"key1": true, "key2": false}
	encodedMapBool, _ := proxycore.EncodeType(datatype.NewMapType(datatype.Varchar, datatype.Boolean), protocalV, mapVarcharToBoolean)

	mapVarcharToVarchar := map[string]string{"key1": "value1", "key2": "value2"}
	encodedMap, _ := proxycore.EncodeType(datatype.NewMapType(datatype.Varchar, datatype.Varchar), protocalV, mapVarcharToVarchar)

	mapVarcharToInt := map[string]int64{"key1": 34, "key2": 66}
	encodedMapInt, _ := proxycore.EncodeType(datatype.NewMapType(datatype.Varchar, datatype.Bigint), protocalV, mapVarcharToInt)

	mapVarcharToBigInt := map[string]int64{"key1": 9878787878, "key2": 6666554433}
	encodedMapBigInt, _ := proxycore.EncodeType(datatype.NewMapType(datatype.Varchar, datatype.Bigint), protocalV, mapVarcharToBigInt)

	mapVarcharToBlob := map[string][]byte{"key1": []byte("blob1"), "key2": []byte("blob2")}
	encodedMapBlob, _ := proxycore.EncodeType(datatype.NewMapType(datatype.Varchar, datatype.Blob), protocalV, mapVarcharToBlob)

	mapVarcharToDate := map[string]time.Time{"key1": time.Now().Truncate(24 * time.Hour).UTC(), "key2": time.Now().AddDate(0, -1, 0).Truncate(24 * time.Hour).UTC()}
	encodedMapDate, _ := proxycore.EncodeType(datatype.NewMapType(datatype.Varchar, datatype.Date), protocalV, mapVarcharToDate)

	mapVarcharToDouble := map[string]float64{"key1": 123.45, "key2": 678.90}
	encodedMapDouble, _ := proxycore.EncodeType(datatype.NewMapType(datatype.Varchar, datatype.Double), protocalV, mapVarcharToDouble)

	mapVarcharInt := map[string]int64{"key1": 123, "key2": 456}
	encodedMapVarcharToInt, _ := proxycore.EncodeType(datatype.NewMapType(datatype.Varchar, datatype.Int), protocalV, mapVarcharInt)
	MapVarcharIntType := datatype.NewMapType(datatype.Varchar, datatype.Int)

	MapVarcharToDoubleType := datatype.NewMapType(datatype.Varchar, datatype.Double)
	MapVarcharToDateType := datatype.NewMapType(datatype.Varchar, datatype.Date)
	MapVarcharToBlobType := datatype.NewMapType(datatype.Varchar, datatype.Blob)
	MapVarcharToVarcharType := datatype.NewMapType(datatype.Varchar, datatype.Varchar)
	MapVarcharToBooleanType := datatype.NewMapType(datatype.Varchar, datatype.Boolean)
	MapVarcharToIntType := datatype.NewMapType(datatype.Varchar, datatype.Bigint)
	MapVarcharToBigIntType := datatype.NewMapType(datatype.Varchar, datatype.Bigint)

	//List Type
	listBoolean := []bool{true, false, true}
	encodedListBoolean, _ := proxycore.EncodeType(datatype.NewListType(datatype.Boolean), protocalV, listBoolean)

	listVarchar := []string{"string1", "string2", "string3"}
	encodedListVarchar, _ := proxycore.EncodeType(datatype.NewListType(datatype.Varchar), protocalV, listVarchar)

	listInt := []int32{123, 456, 789}
	encodedListInt, _ := proxycore.EncodeType(datatype.NewListType(datatype.Int), protocalV, listInt)

	listInt64 := []int64{1239876543, 4543256786, 7890875439}
	encodedListInt64, _ := proxycore.EncodeType(datatype.NewListType(datatype.Bigint), protocalV, listInt64)

	listBlob := [][]byte{[]byte("blob1"), []byte("blob2")}
	encodedListBlob, _ := proxycore.EncodeType(datatype.NewListType(datatype.Blob), protocalV, listBlob)

	listDate := []time.Time{time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)}
	encodedListDate, _ := proxycore.EncodeType(datatype.NewListType(datatype.Date), protocalV, listDate)

	listDouble := []float64{123.456, 789.012}
	encodedListDouble, _ := proxycore.EncodeType(datatype.NewListType(datatype.Double), protocalV, listDouble)

	listTimestamp := []time.Time{time.Now().UTC(), time.Now().Add(time.Hour * 24).UTC()}
	encodedListTimestamp, _ := proxycore.EncodeType(datatype.NewListType(datatype.Timestamp), protocalV, listTimestamp)

	ListTimestampType := datatype.NewListType(datatype.Timestamp)
	ListDoubleType := datatype.NewListType(datatype.Double)
	ListDateType := datatype.NewListType(datatype.Date)
	ListBlobType := datatype.NewListType(datatype.Blob)
	ListIntType := datatype.NewListType(datatype.Int)
	ListInt64Type := datatype.NewListType(datatype.Bigint)
	ListBooleanType := datatype.NewListType(datatype.Boolean)
	ListVarcharType := datatype.NewListType(datatype.Varchar)

	// Set Type

	setBoolean := []bool{true, false, true}
	encodedSetBoolean, _ := proxycore.EncodeType(datatype.NewSetType(datatype.Boolean), protocalV, setBoolean)

	setVarchar := []string{"item1", "item2", "item3"}
	encodedSetVarchar, _ := proxycore.EncodeType(datatype.NewSetType(datatype.Varchar), protocalV, setVarchar)

	setInt := []int32{123, 456, 789}
	encodedSetInt, _ := proxycore.EncodeType(datatype.NewSetType(datatype.Int), protocalV, setInt)

	setBigInt := []int64{123456789012345, 98765432109876, 123456789012345}
	encodedSetBigInt, _ := proxycore.EncodeType(datatype.NewSetType(datatype.Bigint), protocalV, setBigInt)

	setBlob := [][]byte{[]byte("blob1"), []byte("blob2"), []byte("blob1")}
	encodedSetBlob, _ := proxycore.EncodeType(datatype.NewSetType(datatype.Blob), protocalV, setBlob)

	setDate := []time.Time{time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)}
	encodedSetDate, _ := proxycore.EncodeType(datatype.NewSetType(datatype.Date), protocalV, setDate)

	setDouble := []float64{123.456, 789.012, 123.456}
	encodedSetDouble, _ := proxycore.EncodeType(datatype.NewSetType(datatype.Double), protocalV, setDouble)

	setTimestamp := []time.Time{time.Now().UTC(), time.Now().Add(time.Hour * 24).UTC()}
	encodedSetTimestamp, _ := proxycore.EncodeType(datatype.NewSetType(datatype.Timestamp), protocalV, setTimestamp)

	SetTimestampType := datatype.NewSetType(datatype.Timestamp)
	SetDoubleType := datatype.NewSetType(datatype.Double)
	SetDateType := datatype.NewSetType(datatype.Date)
	SetBlobType := datatype.NewSetType(datatype.Blob)
	SetBigIntType := datatype.NewSetType(datatype.Bigint)
	SetIntType := datatype.NewSetType(datatype.Int)
	SetVarcharType := datatype.NewSetType(datatype.Varchar)
	SetBooleanType := datatype.NewSetType(datatype.Boolean)

	tests := []struct {
		name      string
		b         []byte
		choice    datatype.DataType
		protocalV primitive.ProtocolVersion
		want      interface{}
		wantErr   bool
	}{
		{
			name:      "Decode Varchar",
			b:         []byte("test varchar"),
			choice:    datatype.Varchar,
			protocalV: protocalV,
			want:      "test varchar",
			wantErr:   false,
		},
		{
			name:      "Decode Double",
			b:         []byte{64, 94, 221, 47, 26, 159, 190, 77},
			choice:    datatype.Double,
			protocalV: protocalV,
			want:      123.46,
			wantErr:   false,
		},
		{
			name:      "Decode Bigint",
			b:         []byte{0, 0, 0, 0, 0, 0, 0, 123},
			choice:    datatype.Bigint,
			protocalV: protocalV,
			want:      int64(123),
			wantErr:   false,
		},
		{
			name:      "Decode Timestamp",
			b:         tm,
			choice:    datatype.Timestamp,
			protocalV: protocalV,
			want:      time.Unix(1234567890, 0).UTC(),
			wantErr:   false,
		},
		{
			name:      "Decode Int",
			b:         []byte{0, 0, 0, 123},
			choice:    datatype.Int,
			protocalV: protocalV,
			want:      int64(123),
			wantErr:   false,
		},
		{
			name:      "Decode Boolean",
			b:         boolBytes,
			choice:    datatype.Boolean,
			protocalV: protocalV,
			want:      true,
			wantErr:   false,
		},
		{
			name:      "Decode Date",
			b:         dateBytes,
			choice:    datatype.Date,
			protocalV: protocalV,
			want:      dateValue,
			wantErr:   false,
		},
		{
			name:      "Decode Blob",
			b:         blobBytes,
			choice:    datatype.Blob,
			protocalV: protocalV,
			want:      blobValue,
			wantErr:   false,
		},
		{
			name:      "Decode map<varchar,boolean>",
			b:         encodedMapBool,
			choice:    datatype.NewMapType(datatype.Varchar, datatype.Boolean),
			protocalV: protocalV,
			want:      mapVarcharToBoolean,
			wantErr:   false,
		},
		{
			name:      "Decode map<varchar,varchar>",
			b:         encodedMap,
			choice:    datatype.NewMapType(datatype.Varchar, datatype.Varchar),
			protocalV: protocalV,
			want:      mapVarcharToVarchar,
			wantErr:   false,
		},
		{
			name:      "Decode map<varchar,Int>",
			b:         encodedMapInt,
			choice:    datatype.NewMapType(datatype.Varchar, datatype.Bigint),
			protocalV: protocalV,
			want:      mapVarcharToInt,
			wantErr:   false,
		},
		{
			name:      "Decode map<varchar,BigInt>",
			b:         encodedMapBigInt,
			choice:    datatype.NewMapType(datatype.Varchar, datatype.Bigint),
			protocalV: protocalV,
			want:      mapVarcharToBigInt,
			wantErr:   false,
		},
		{
			name:      "Decode map<varchar,blob>",
			b:         encodedMapBlob,
			choice:    MapVarcharToBlobType,
			protocalV: protocalV,
			want:      mapVarcharToBlob,
			wantErr:   false,
		},
		{
			name:      "Decode map<varchar,date>",
			b:         encodedMapDate,
			choice:    MapVarcharToDateType,
			protocalV: protocalV,
			want:      mapVarcharToDate,
			wantErr:   false,
		},
		{
			name:      "Decode map<varchar,double>",
			b:         encodedMapDouble,
			choice:    MapVarcharToDoubleType,
			protocalV: protocalV,
			want:      mapVarcharToDouble,
			wantErr:   false,
		},
		{
			name:      "Decode list<boolean>",
			b:         encodedListBoolean,
			choice:    ListBooleanType,
			protocalV: protocalV,
			want:      listBoolean,
			wantErr:   false,
		},
		{
			name:      "Decode list<varchar>",
			b:         encodedListVarchar,
			choice:    ListVarcharType,
			protocalV: protocalV,
			want:      listVarchar,
			wantErr:   false,
		},
		{
			name:      "Decode list<Int>",
			b:         encodedListInt,
			choice:    ListIntType,
			protocalV: protocalV,
			want:      listInt,
			wantErr:   false,
		},
		{
			name:      "Decode list<Bigint>",
			b:         encodedListInt64,
			choice:    ListInt64Type,
			protocalV: protocalV,
			want:      listInt64,
			wantErr:   false,
		},
		{
			name:      "Decode list<blob>",
			b:         encodedListBlob,
			choice:    ListBlobType,
			protocalV: protocalV,
			want:      listBlob,
			wantErr:   false,
		},
		{
			name:      "Decode list<date>",
			b:         encodedListDate,
			choice:    ListDateType,
			protocalV: protocalV,
			want:      listDate,
			wantErr:   false,
		},
		{
			name:      "Decode list<double>",
			b:         encodedListDouble,
			choice:    ListDoubleType,
			protocalV: protocalV,
			want:      listDouble,
			wantErr:   false,
		},
		{
			name:      "Decode set<boolean>",
			b:         encodedSetBoolean,
			choice:    SetBooleanType,
			protocalV: protocalV,
			want:      setBoolean,
			wantErr:   false,
		},
		{
			name:      "Decode set<varchar>",
			b:         encodedSetVarchar,
			choice:    SetVarcharType,
			protocalV: protocalV,
			want:      setVarchar,
			wantErr:   false,
		},
		{
			name:      "Decode set<int>",
			b:         encodedSetInt,
			choice:    SetIntType,
			protocalV: protocalV,
			want:      setInt,
			wantErr:   false,
		},
		{
			name:      "Decode set<bigint>",
			b:         encodedSetBigInt,
			choice:    SetBigIntType,
			protocalV: protocalV,
			want:      setBigInt,
			wantErr:   false,
		},
		{
			name:      "Decode set<blob>",
			b:         encodedSetBlob,
			choice:    SetBlobType,
			protocalV: protocalV,
			want:      setBlob,
			wantErr:   false,
		},
		{
			name:      "Decode set<date>",
			b:         encodedSetDate,
			choice:    SetDateType,
			protocalV: protocalV,
			want:      setDate,
			wantErr:   false,
		},
		{
			name:      "Decode set<double>",
			b:         encodedSetDouble,
			choice:    SetDoubleType,
			protocalV: protocalV,
			want:      setDouble,
			wantErr:   false,
		},
		{
			name:      "Decode set<timestamp>",
			b:         encodedSetTimestamp,
			choice:    SetTimestampType,
			protocalV: protocalV,
			want:      setTimestamp,
			wantErr:   false,
		},
		{
			name:      "Decode list<timestamp>",
			b:         encodedListTimestamp,
			choice:    ListTimestampType,
			protocalV: protocalV,
			want:      listTimestamp,
			wantErr:   false,
		},
		{
			name:      "Decode map<varchar,int>",
			b:         encodedMapVarcharToInt,
			choice:    MapVarcharIntType,
			protocalV: protocalV,
			want:      mapVarcharInt,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeBytesToSpannerColumnType(tt.b, tt.choice, tt.protocalV)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeBytesToSpannerColumnType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.choice.String() == MapVarcharToVarcharType.String() ||
				tt.choice.String() == MapVarcharToBooleanType.String() ||
				tt.choice.String() == MapVarcharToIntType.String() ||
				tt.choice.String() == MapVarcharIntType.String() ||
				tt.choice.String() == MapVarcharToBigIntType.String() ||
				tt.choice.String() == MapVarcharToDateType.String() ||
				tt.choice.String() == MapVarcharToDoubleType.String() ||
				tt.choice.String() == MapVarcharToBlobType.String() ||
				tt.choice.String() == ListBooleanType.String() ||
				tt.choice.String() == ListIntType.String() ||
				tt.choice.String() == ListInt64Type.String() ||
				tt.choice.String() == ListBlobType.String() ||
				tt.choice.String() == ListDateType.String() ||
				tt.choice.String() == ListDoubleType.String() ||
				tt.choice.String() == ListVarcharType.String() ||
				tt.choice.String() == SetVarcharType.String() ||
				tt.choice.String() == SetBooleanType.String() ||
				tt.choice.String() == SetBigIntType.String() ||
				tt.choice.String() == SetBlobType.String() ||
				tt.choice.String() == SetDateType.String() ||
				tt.choice.String() == SetDoubleType.String() ||
				tt.choice.String() == SetIntType.String() {
				gotJSONBytes, err := json.Marshal(got)
				if err != nil {
					t.Errorf("Failed to marshal 'got' spanner.NullJSON to JSON: %v", err)
					return
				}

				wantJSONBytes, err := json.Marshal(tt.want)
				if err != nil {
					t.Errorf("Failed to marshal 'want' map to JSON: %v", err)
					return
				}

				if !jsonEqual(string(gotJSONBytes), string(wantJSONBytes)) {
					t.Errorf("DecodeBytesToSpannerColumnType() got = %v, want %v", string(gotJSONBytes), string(wantJSONBytes))
				}
			} else if tt.choice.String() == SetTimestampType.String() || tt.choice.String() == ListTimestampType.String() {
				gotTimestampPtrs, ok := got.([]*time.Time)
				if !ok {
					t.Fatalf("Type assertion failed for []*time.Time")
				}

				// Convert []*time.Time to []time.Time with truncation
				gotTimestamps := make([]time.Time, len(gotTimestampPtrs))
				for i, ptr := range gotTimestampPtrs {
					if ptr != nil {
						gotTimestamps[i] = truncateTimeToMillis(*ptr)
					}
				}

				wantTimestamps := make([]time.Time, len(tt.want.([]time.Time)))
				for i, wt := range tt.want.([]time.Time) {
					wantTimestamps[i] = truncateTimeToMillis(wt)
				}

				if !reflect.DeepEqual(gotTimestamps, wantTimestamps) {
					t.Errorf("Mismatch: got %v, want %v", gotTimestamps, wantTimestamps)
				}
			} else if tt.choice == datatype.Double {
				gotFloat, ok := got.(float64)
				if !ok {
					t.Errorf("DecodeBytesToSpannerColumnType() type assertion to float64 failed")
					return
				}
				gotRounded := math.Round(gotFloat*100) / 100
				wantRounded := math.Round(tt.want.(float64)*100) / 100
				if gotRounded != wantRounded {
					t.Errorf("DecodeBytesToSpannerColumnType() got = %v, want %v", gotRounded, wantRounded)
				}
			} else if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DecodeBytesToSpannerColumnType() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestKeyExistsInList tests the KeyExistsInList function with various inputs.
func TestKeyExistsInList(t *testing.T) {
	cases := []struct {
		name     string
		key      string
		list     []string
		expected bool
	}{
		{
			name:     "Key present in list",
			key:      "banana",
			list:     []string{"apple", "banana", "cherry"},
			expected: true,
		},
		{
			name:     "Key not present in list",
			key:      "mango",
			list:     []string{"apple", "banana", "cherry"},
			expected: false,
		},
		{
			name:     "Empty list",
			key:      "banana",
			list:     []string{},
			expected: false,
		},
		{
			name:     "Nil list",
			key:      "banana",
			list:     nil,
			expected: false,
		},
		{
			name:     "Key at the beginning",
			key:      "apple",
			list:     []string{"apple", "banana", "cherry"},
			expected: true,
		},
		{
			name:     "Key at the end",
			key:      "cherry",
			list:     []string{"apple", "banana", "cherry"},
			expected: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := KeyExistsInList(tc.key, tc.list)
			if got != tc.expected {
				t.Errorf("%s: expected %v, got %v", tc.name, tc.expected, got)
			}
		})
	}
}

// TestUnixToISO tests the unixToISO function by checking if it correctly formats a known Unix timestamp.
func TestUnixToISO(t *testing.T) {
	// Known Unix timestamp and its corresponding ISO 8601 representation
	unixTimestamp := int64(1609459200) // 2021-01-01T00:00:00Z
	expectedISO := "2021-01-01T00:00:00Z"

	isoTimestamp := unixToISO(unixTimestamp)
	if isoTimestamp != expectedISO {
		t.Errorf("unixToISO(%d) = %s; want %s", unixTimestamp, isoTimestamp, expectedISO)
	}
}

// TestAddSecondsToCurrentTimestamp tests the addSecondsToCurrentTimestamp function by checking the format of the output.
func TestAddSecondsToCurrentTimestamp(t *testing.T) {
	secondsToAdd := int64(3600) // 1 hour
	result := AddSecondsToCurrentTimestamp(secondsToAdd)

	// Parse the result to check if it's in correct ISO 8601 format
	if _, err := time.Parse(time.RFC3339, result); err != nil {
		t.Errorf("AddSecondsToCurrentTimestamp(%d) returned an incorrectly formatted time: %s", secondsToAdd, result)
	}
}

// TestExtractAfterWhere tests the extractAfterWhere function with various SQL queries.
func TestExtractAfterWhere(t *testing.T) {
	tests := []struct {
		name     string
		sqlQuery string
		want     string
		wantErr  bool
	}{
		{
			name:     "Valid query with uppercase WHERE",
			sqlQuery: "SELECT * FROM table WHERE column = 'value';",
			want:     "column = 'value';",
			wantErr:  false,
		},
		{
			name:     "Valid query with lowercase where",
			sqlQuery: "SELECT * FROM table where column = 'value';",
			want:     "column = 'value';",
			wantErr:  false,
		},
		{
			name:     "No WHERE clause",
			sqlQuery: "SELECT * FROM table;",
			want:     "",
			wantErr:  true,
		},
		{
			name:     "Mixed case WHERE",
			sqlQuery: "SELECT * FROM table WhErE XOZO = 'value';",
			want:     "XOZO = 'value';",
			wantErr:  false,
		},
		{
			name:     "Complex query with subquery",
			sqlQuery: "UPDATE table SET column = 'value' WHERE ID IN (SELECT id FROM table2 WHERE column2 = 'value2');",
			want:     "ID IN (SELECT id FROM table2 WHERE column2 = 'value2');",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractAfterWhere(tt.sqlQuery)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractAfterWhere() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ExtractAfterWhere() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSystemSchemaRawCachedValues(t *testing.T) {
	tests := []struct {
		name    string
		hdr     *frame.Header
		s       [][]interface{}
		want    [][][]byte
		wantErr bool
	}{
		{
			name: "Basic valid case",
			hdr:  &frame.Header{Version: 1},
			s: [][]interface{}{
				{"text", 123, true},
				{456, "more text", false},
			},
			want: [][][]byte{
				{[]byte("text"), {0, 0, 0, 123}, {1}},
				{{0, 0, 1, 200}, []byte("more text"), {0}},
			},
			wantErr: false,
		},
		{
			name: "Invalid type conversion case",
			hdr:  &frame.Header{Version: 1},
			s: [][]interface{}{
				{make(chan int), 123}, // Invalid type for conversion
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Empty slice case",
			hdr:     &frame.Header{Version: 1},
			s:       [][]interface{}{},
			want:    [][][]byte{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getSystemSchemaRawCachedValues(tt.hdr, tt.s)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestNewCred(t *testing.T) {
	tests := []struct {
		name           string
		caCertPath     string
		clientCertPath string
		clientKeyPath  string
		wantErr        bool
	}{
		{
			name:           "Missing CA certificate",
			caCertPath:     "",
			clientCertPath: "",
			clientKeyPath:  "",
			wantErr:        true,
		},
		{
			name:           "Invalid CA certificate path",
			caCertPath:     "invalid/path/to/ca.crt",
			clientCertPath: "",
			clientKeyPath:  "",
			wantErr:        true,
		},
		{
			name:           "mTLS: Missing client key",
			caCertPath:     "testdata/ca.crt",
			clientCertPath: "testdata/client.crt",
			clientKeyPath:  "",
			wantErr:        true,
		},
		{
			name:           "mTLS: Missing client cert",
			caCertPath:     "testdata/ca.crt",
			clientCertPath: "",
			clientKeyPath:  "testdata/client.key",
			wantErr:        true,
		},
		{
			name:           "TLS: Valid CA certificate",
			caCertPath:     "testdata/ca.crt",
			clientCertPath: "",
			clientKeyPath:  "",
			wantErr:        false,
		},
		{
			name:           "mTLS: Valid CA, client cert, and key",
			caCertPath:     "testdata/ca.crt",
			clientCertPath: "testdata/client.crt",
			clientKeyPath:  "testdata/client.key",
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cred, err := NewCred(tt.caCertPath, tt.clientCertPath, tt.clientKeyPath)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, cred)
				_, ok := cred.(credentials.TransportCredentials)
				assert.True(t, ok, "Expected TransportCredentials")
			}
		})
	}
}
