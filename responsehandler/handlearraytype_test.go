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

package responsehandler_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/ollionorg/cassandra-to-spanner-proxy/responsehandler"
	"github.com/ollionorg/cassandra-to-spanner-proxy/utilities"
	"github.com/tj/assert"
	"go.uber.org/zap"
)

func TestTypeHandler_HandleInt64ArrayType(t *testing.T) {
	th := &responsehandler.TypeHandler{ProtocalV: primitive.ProtocolVersion4}

	type args struct {
		row *spanner.Row
		i   int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "Success case list type",
			args: args{
				row: mockSpannerRowWithIntArray(),
				i:   0,
			},
			want:    convertIntArrayToBytes([]int64{1, 2, 3, 4}),
			wantErr: false,
		},
		{
			name: "Success case set type",
			args: args{
				row: mockSpannerRowWithIntArray(),
				i:   0,
			},
			want:    convertIntArrayToBytes([]int64{1, 2, 3, 4}),
			wantErr: false,
		},
		{
			name: "Error case",
			args: args{
				row: mockSpannerRowWithError(),
				i:   0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := th.HandleInt64ArrayType(tt.args.i, tt.args.row)
			if !tt.wantErr {
				if err != nil {
					t.Errorf("TypeHandler.handleInt64ArrayType() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(result, tt.want) {
					t.Errorf("TypeHandler.handleInt64ArrayType() got1 = %v, want %v", result, tt.want)
				}
			} else if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}

func TestTypeHandler_HandleInt32ArrayType(t *testing.T) {
	th := &responsehandler.TypeHandler{ProtocalV: primitive.ProtocolVersion4}

	type args struct {
		row *spanner.Row
		i   int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "Success case list type",
			args: args{
				row: mockSpannerRowWithIntArray(),
				i:   0,
			},
			want:    convertInt32ArrayToBytes([]int32{1, 2, 3, 4}),
			wantErr: false,
		},
		{
			name: "Success case set type",
			args: args{
				row: mockSpannerRowWithIntArray(),
				i:   0,
			},
			want:    convertInt32ArrayToBytes([]int32{1, 2, 3, 4}),
			wantErr: false,
		},
		{
			name: "Error case",
			args: args{
				row: mockSpannerRowWithError(),
				i:   0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := th.HandleInt32ArrayType(tt.args.i, tt.args.row)
			if !tt.wantErr {
				if err != nil {
					t.Errorf("TypeHandler.handleInt64ArrayType() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(result, tt.want) {
					t.Errorf("TypeHandler.handleInt64ArrayType() got1 = %v, want %v", result, tt.want)
				}
			} else if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}

func TestTypeHandler_HandleDoubleArrayType(t *testing.T) {
	th := &responsehandler.TypeHandler{ProtocalV: primitive.ProtocolVersion4}

	type args struct {
		row *spanner.Row
		i   int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "Success case list type",
			args: args{
				row: mockSpannerRowWithDoubleArray(),
				i:   0,
			},
			want:    convertFloat64ArrayToBytes([]float64{1.5, 2.2, 3.7, 4.2}),
			wantErr: false,
		},
		{
			name: "Success case set type",
			args: args{
				row: mockSpannerRowWithDoubleArray(),
				i:   0,
			},
			want:    convertFloat64ArrayToBytes([]float64{1.5, 2.2, 3.7, 4.2}),
			wantErr: false,
		},
		{
			name: "Error case",
			args: args{
				row: mockSpannerRowWithError(),
				i:   0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := th.HandleFloat64ArrayType(tt.args.i, tt.args.row)
			if !tt.wantErr {
				if err != nil {
					t.Errorf("TypeHandler.handleInt64ArrayType() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(result, tt.want) {
					t.Errorf("TypeHandler.handleInt64ArrayType() got1 = %v, want %v", result, tt.want)
				}
			} else if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}

func TestTypeHandler_HandleFloatArrayType(t *testing.T) {
	th := &responsehandler.TypeHandler{ProtocalV: primitive.ProtocolVersion4}

	type args struct {
		row *spanner.Row
		i   int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "Success case list type",
			args: args{
				row: mockSpannerRowWithDoubleArray(),
				i:   0,
			},
			want:    convertFloat32ArrayToBytes([]float32{1.5, 2.2, 3.7, 4.2}),
			wantErr: false,
		},
		{
			name: "Success case set type",
			args: args{
				row: mockSpannerRowWithDoubleArray(),
				i:   0,
			},
			want:    convertFloat32ArrayToBytes([]float32{1.5, 2.2, 3.7, 4.2}),
			wantErr: false,
		},
		{
			name: "Error case",
			args: args{
				row: mockSpannerRowWithError(),
				i:   0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := th.HandleFloatArrayType(tt.args.i, tt.args.row)
			if !tt.wantErr {
				if err != nil {
					t.Errorf("TypeHandler.handleInt64ArrayType() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(result, tt.want) {
					t.Errorf("TypeHandler.handleInt64ArrayType() got1 = %v, want %v", result, tt.want)
				}
			} else if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}
func TestTypeHandler_HandleStringArrayType(t *testing.T) {
	th := &responsehandler.TypeHandler{ProtocalV: primitive.ProtocolVersion4}

	type args struct {
		row *spanner.Row
		i   int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "Success case list type",
			args: args{
				row: mockSpannerRowWithStringArray(),
				i:   0,
			},
			want:    convertStringArrayToBytes([]string{"a", "b", "c", "d"}),
			wantErr: false,
		},
		{
			name: "Success case set type",
			args: args{
				row: mockSpannerRowWithStringArray(),
				i:   0,
			},
			want:    convertStringArrayToBytes([]string{"a", "b", "c", "d"}),
			wantErr: false,
		},
		{
			name: "Error case",
			args: args{
				row: mockSpannerRowWithError(),
				i:   0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := th.HandleStringArrayType(tt.args.i, tt.args.row)
			if !tt.wantErr {
				if err != nil {
					t.Errorf("TypeHandler.handleInt64ArrayType() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(result, tt.want) {
					t.Errorf("TypeHandler.handleInt64ArrayType() got1 = %v, want %v", result, tt.want)
				}
			} else if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}

func TestTypeHandler_HandleBoolArrayType(t *testing.T) {
	th := &responsehandler.TypeHandler{ProtocalV: primitive.ProtocolVersion4}
	type args struct {
		row *spanner.Row
		i   int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "Success case list type",
			args: args{
				row: mockSpannerRowWithBoolArray(),
				i:   0,
			},
			want:    convertBoolArrayToBytes([]bool{true, false, true, true}),
			wantErr: false,
		},
		{
			name: "Success case set type",
			args: args{
				row: mockSpannerRowWithBoolArray(),
				i:   0,
			},
			want:    convertBoolArrayToBytes([]bool{true, false, true, true}),
			wantErr: false,
		},
		{
			name: "Error case",
			args: args{
				row: mockSpannerRowWithError(),
				i:   0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := th.HandleBoolArrayType(tt.args.i, tt.args.row)
			if !tt.wantErr {
				if err != nil {
					t.Errorf("TypeHandler.handleInt64ArrayType() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(result, tt.want) {
					t.Errorf("TypeHandler.handleInt64ArrayType() got1 = %v, want %v", result, tt.want)
				}
			} else if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}

func TestTypeHandler_HandleTimestampArrayType(t *testing.T) {
	th := &responsehandler.TypeHandler{ProtocalV: primitive.ProtocolVersion4}
	type args struct {
		row *spanner.Row
		i   int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "Success case list type",
			args: args{
				row: mockSpannerRowWithTimestampArray(),
				i:   0,
			},
			want: convertTimestampArrayToBytes([]time.Time{
				time.Date(2023, 1, 15, 12, 30, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 13, 45, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 15, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 16, 15, 0, 0, time.UTC),
			}),
			wantErr: false,
		},
		{
			name: "Success case set type",
			args: args{
				row: mockSpannerRowWithTimestampArray(),
				i:   0,
			},
			want: convertTimestampArrayToBytes([]time.Time{
				time.Date(2023, 1, 15, 12, 30, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 13, 45, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 15, 0, 0, 0, time.UTC),
				time.Date(2023, 1, 15, 16, 15, 0, 0, time.UTC),
			}),
			wantErr: false,
		},
		{
			name: "Error case",
			args: args{
				row: mockSpannerRowWithError(),
				i:   0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			result, err := th.HandleTimestampArrayType(tt.args.i, tt.args.row)
			if !tt.wantErr {
				if err != nil {
					t.Errorf("TypeHandler.handleInt64ArrayType() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(result, tt.want) {
					t.Errorf("TypeHandler.handleInt64ArrayType() got1 = %v, want %v", result, tt.want)
				}
			} else if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}

func TestTypeHandler_HandleByteArrayType(t *testing.T) {
	th := &responsehandler.TypeHandler{ProtocalV: primitive.ProtocolVersion4}
	type args struct {
		row *spanner.Row
		i   int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "Success case list type",
			args: args{
				row: mockSpannerRowWithByteArray(),
				i:   0,
			},
			want: convertByteArrayToBytes([][]byte{
				{0x01, 0x02, 0x03},
				{0x04, 0x05, 0x06, 0x07},
				{0x08, 0x09},
				{0x00},
			}),
			wantErr: false,
		},
		{
			name: "Success case set type",
			args: args{
				row: mockSpannerRowWithByteArray(),
				i:   0,
			},
			want: convertByteArrayToBytes([][]byte{
				{0x01, 0x02, 0x03},
				{0x04, 0x05, 0x06, 0x07},
				{0x08, 0x09},
				{0x00},
			}),
			wantErr: false,
		},
		{
			name: "Error case",
			args: args{
				row: mockSpannerRowWithError(),
				i:   0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			result, err := th.HandleByteArrayType(tt.args.i, tt.args.row)
			if !tt.wantErr {
				if err != nil {
					t.Errorf("TypeHandler.handleInt64ArrayType() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(result, tt.want) {
					t.Errorf("TypeHandler.handleInt64ArrayType() got1 = %v, want %v", result, tt.want)
				}
			} else if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}

func Test_convertCivilDateToTime(t *testing.T) {
	type args struct {
		cDate civil.Date
	}
	tests := []struct {
		name string
		args args
		want time.Time
	}{
		{
			name: "Valid date",
			args: args{
				cDate: civil.Date{Year: 2023, Month: 12, Day: 5},
			},
			want: time.Date(2023, time.December, 5, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := responsehandler.ConvertCivilDateToTime(tt.args.cDate); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertCivilDateToTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConvertToCQLDate(t *testing.T) {
	type args struct {
		t time.Time
	}
	tests := []struct {
		name string
		args args
		want int32
	}{
		{
			name: "Converts to CQL date - case 1",
			args: args{t: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
			want: 0,
		},
		{
			name: "Converts to CQL date - case 2",
			args: args{t: time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC)},
			want: 19722,
		},
		{
			name: "Converts to CQL date - case 3",
			args: args{t: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)},
			want: 18993,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := responsehandler.ConvertToCQLDate(tt.args.t); got != tt.want {
				t.Errorf("ConvertToCQLDate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTypeHandler_HandleDateArrayType(t *testing.T) {
	th := &responsehandler.TypeHandler{ProtocalV: primitive.ProtocolVersion4}
	type args struct {
		row *spanner.Row
		i   int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "Success case list type",
			args: args{
				row: mockSpannerRowWithDateArray(),
				i:   0,
			},
			want: convertDateArrayToBytes([]spanner.NullDate{
				{Date: civil.Date{Year: 2023, Month: 1, Day: 15}, Valid: true},
				{Date: civil.Date{Year: 1700, Month: 14, Day: 40}, Valid: false},
			}, utilities.ListDateCassandraType),
			wantErr: false,
		},
		{
			name: "Success case set type",
			args: args{
				row: mockSpannerRowWithDateArray(),
				i:   0,
			},
			want: convertDateArrayToBytes([]spanner.NullDate{
				{Date: civil.Date{Year: 2023, Month: 1, Day: 15}, Valid: true},
				{Date: civil.Date{Year: 1700, Month: 14, Day: 40}, Valid: false},
			}, utilities.ListDateCassandraType),
			wantErr: false,
		},
		{
			name: "Error case",
			args: args{
				row: mockSpannerRowWithError(),
				i:   0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			result, err := th.HandleDateArrayType(tt.args.i, tt.args.row)
			if !tt.wantErr {
				if err != nil {
					t.Errorf("TypeHandler.handleInt64ArrayType() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(result, tt.want) {
					t.Errorf("TypeHandler.handleInt64ArrayType() got1 = %v, want %v", result, tt.want)
				}
			} else if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}

func TestTypeHandler_HandleDateSetType(t *testing.T) {
	th := &responsehandler.TypeHandler{ProtocalV: primitive.ProtocolVersion4}
	type args struct {
		row *spanner.Row
		i   int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "Success case list type",
			args: args{
				row: mockSpannerRowWithDateArray(),
				i:   0,
			},
			want: convertDateArrayToBytes([]spanner.NullDate{
				{Date: civil.Date{Year: 2023, Month: 1, Day: 15}, Valid: true},
				{Date: civil.Date{Year: 1700, Month: 14, Day: 40}, Valid: false},
			}, utilities.SetDateCassandraType),
			wantErr: false,
		},
		{
			name: "Success case set type",
			args: args{
				row: mockSpannerRowWithDateArray(),
				i:   0,
			},
			want: convertDateArrayToBytes([]spanner.NullDate{
				{Date: civil.Date{Year: 2023, Month: 1, Day: 15}, Valid: true},
				{Date: civil.Date{Year: 1700, Month: 14, Day: 40}, Valid: false},
			}, utilities.SetDateCassandraType),
			wantErr: false,
		},
		{
			name: "Error case",
			args: args{
				row: mockSpannerRowWithError(),
				i:   0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			result, err := th.HandleDateArrayType(tt.args.i, tt.args.row)
			if !tt.wantErr {
				if err != nil {
					t.Errorf("TypeHandler.handleInt64ArrayType() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(result, tt.want) {
					t.Errorf("TypeHandler.handleInt64ArrayType() got1 = %v, want %v", result, tt.want)
				}
			} else if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}
func TestTypeHandler_HandleArrayType(t *testing.T) {
	th := &responsehandler.TypeHandler{Logger: zap.NewNop(), ProtocalV: primitive.ProtocolVersion4}

	tests := []struct {
		name         string
		dataType     datatype.DataType
		expectedFunc func(int, *spanner.Row) ([]byte, error)
		expectedErr  error
	}{
		{
			name:         "Int64 List",
			dataType:     utilities.ListBigintCassandraType,
			expectedFunc: th.HandleInt64ArrayType,
			expectedErr:  nil,
		},
		{
			name:         "Int64 Set",
			dataType:     utilities.SetBigintCassandraType,
			expectedFunc: th.HandleInt64SetType,
			expectedErr:  nil,
		},
		{
			name:         "Varchar List",
			dataType:     utilities.ListTextCassandraType,
			expectedFunc: th.HandleStringArrayType,
			expectedErr:  nil,
		},
		{
			name:         "Varchar Set",
			dataType:     utilities.SetTextCassandraType,
			expectedFunc: th.HandleStringSetType,
			expectedErr:  nil,
		},
		{
			name:         "Int32 List",
			dataType:     utilities.ListIntCassandraType,
			expectedFunc: th.HandleInt32ArrayType,
			expectedErr:  nil,
		},
		{
			name:         "Int32 Set",
			dataType:     utilities.SetIntCassandraType,
			expectedFunc: th.HandleInt32SetType,
			expectedErr:  nil,
		},
		{
			name:         "Boolean list",
			dataType:     utilities.ListBooleanCassandraType,
			expectedFunc: th.HandleBoolArrayType,
			expectedErr:  nil,
		},
		{
			name:         "Boolean Set",
			dataType:     utilities.SetBooleanCassandraType,
			expectedFunc: th.HandleBoolSetType,
			expectedErr:  nil,
		},
		{
			name:         "Float64 List",
			dataType:     utilities.ListDoubleCassandraType,
			expectedFunc: th.HandleFloat64ArrayType,
			expectedErr:  nil,
		},
		{
			name:         "Float64 Set",
			dataType:     utilities.SetDoubleCassandraType,
			expectedFunc: th.HandleFloat64SetType,
			expectedErr:  nil,
		},
		{
			name:         "Float List",
			dataType:     utilities.ListFloatCassandraType,
			expectedFunc: th.HandleFloatArrayType,
			expectedErr:  nil,
		},
		{
			name:         "Float Set",
			dataType:     utilities.SetFloatCassandraType,
			expectedFunc: th.HandleFloatSetType,
			expectedErr:  nil,
		},
		{
			name:         "Date List",
			dataType:     utilities.ListDateCassandraType,
			expectedFunc: th.HandleDateArrayType,
			expectedErr:  nil,
		},
		{
			name:         "Date Set",
			dataType:     utilities.SetDateCassandraType,
			expectedFunc: th.HandleDateSetType,
			expectedErr:  nil,
		},
		{
			name:         "Timestamp List",
			dataType:     utilities.ListTimestampCassandraType,
			expectedFunc: th.HandleTimestampArrayType,
			expectedErr:  nil,
		},
		{
			name:         "Timestamp Set",
			dataType:     utilities.SetTimestampCassandraType,
			expectedFunc: th.HandleTimestampSetType,
			expectedErr:  nil,
		},
		{
			name:         "Blob List",
			dataType:     utilities.ListBlobCassandraType,
			expectedFunc: th.HandleByteArrayType,
			expectedErr:  nil,
		},
		{
			name:         "Blob Set",
			dataType:     utilities.SetBlobCassandraType,
			expectedFunc: th.HandleByteSetType,
			expectedErr:  nil,
		},
		{
			name:         "Unsupported Type",
			dataType:     datatype.Int, // Invalid type
			expectedFunc: nil,
			expectedErr:  fmt.Errorf("unsupported element type: %v", datatype.Int),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultFunc, resultErr := th.HandleArrayType(tt.dataType)

			if tt.expectedErr == nil {
				assert.NoError(t, resultErr)
				assert.Equal(t, fmt.Sprintf("%p", tt.expectedFunc), fmt.Sprintf("%p", resultFunc))
			} else {
				assert.EqualError(t, resultErr, tt.expectedErr.Error())
				assert.Nil(t, resultFunc)
			}
		})
	}
}
