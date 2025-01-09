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

package otelgo

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

func TestNewOpenTelemetry(t *testing.T) {
	ctx := context.Background()
	srv1 := setupTestEndpoint(":7060", "/trace")
	srv2 := setupTestEndpoint(":7061", "/metric")
	srv := setupTestEndpoint(":7062", "/TestNewOpenTelemetry")
	time.Sleep(3 * time.Second)
	defer func() {
		assert.NoError(t, srv.Shutdown(ctx), "failed to shutdown srv")
		assert.NoError(t, srv1.Shutdown(ctx), "failed to shutdown srv1")
		assert.NoError(t, srv2.Shutdown(ctx), "failed to shutdown srv2")
	}()
	type args struct {
		ctx    context.Context
		config *OTelConfig
		logger *zap.Logger
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Test success",
			args: args{
				ctx: ctx,
				config: &OTelConfig{
					TracerEndpoint:   "http://localhost:7060",
					MetricEndpoint:   "http://localhost:7061",
					ServiceName:      "test",
					OTELEnabled:      true,
					TraceSampleRatio: 20,
					Database:         "testDB",
					Instance:         "testInstance",
					HealthCheckEp:    "localhost:7062/TestNewOpenTelemetry",
				},
				logger: zap.NewNop(),
			},
			wantErr: false,
		},
		{
			name: "Test when otel disabled",
			args: args{
				ctx: ctx,
				config: &OTelConfig{
					TracerEndpoint:   "http://localhost:7060",
					MetricEndpoint:   "http://localhost:7061",
					ServiceName:      "test",
					OTELEnabled:      false,
					TraceSampleRatio: 20,
					Database:         "testDB",
					Instance:         "testInstance",
					HealthCheckEp:    "localhost:7062/TestNewOpenTelemetry",
				},
				logger: zap.NewNop(),
			},
			wantErr: false,
		},
		{
			name: "Test when healthcheck endpoint missing",
			args: args{
				ctx: ctx,
				config: &OTelConfig{
					TracerEndpoint:     "http://localhost:7060",
					MetricEndpoint:     "http://localhost:7061",
					ServiceName:        "test",
					OTELEnabled:        true,
					TraceSampleRatio:   20,
					Database:           "testDB",
					Instance:           "testInstance",
					HealthCheckEnabled: false,
					HealthCheckEp:      "",
				},
				logger: zap.NewNop(),
			},
			wantErr: false,
		},
		{
			name: "Test error when healthcheck endpoint missing",
			args: args{
				ctx: ctx,
				config: &OTelConfig{
					TracerEndpoint:     "http://localhost:7060",
					MetricEndpoint:     "http://localhost:7061",
					ServiceName:        "test",
					OTELEnabled:        true,
					TraceSampleRatio:   20,
					Database:           "testDB",
					Instance:           "testInstance",
					HealthCheckEnabled: true,
					HealthCheckEp:      "",
				},
				logger: zap.NewNop(),
			},
			wantErr: true,
		},
		{
			name: "Test error when TracerEndpoint endpoint missing",
			args: args{
				ctx: ctx,
				config: &OTelConfig{
					TraceEnabled:     true,
					TracerEndpoint:   "",
					MetricEndpoint:   "http://localhost:7061",
					ServiceName:      "test",
					OTELEnabled:      true,
					TraceSampleRatio: 20,
					Database:         "testDB",
					Instance:         "testInstance",
					HealthCheckEp:    "localhost:7062/TestNewOpenTelemetry",
				},
				logger: zap.NewNop(),
			},
			wantErr: true,
		},
		{
			name: "Test when MetricEndpoint endpoint missing",
			args: args{
				ctx: ctx,
				config: &OTelConfig{
					MetricEnabled:    true,
					TracerEndpoint:   "http://localhost:7060",
					MetricEndpoint:   "",
					ServiceName:      "test",
					OTELEnabled:      true,
					TraceSampleRatio: 20,
					Database:         "testDB",
					Instance:         "testInstance",
					HealthCheckEp:    "localhost:7062/TestNewOpenTelemetry",
				},
				logger: zap.NewNop(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			_, _, err := NewOpenTelemetry(tt.args.ctx, tt.args.config, tt.args.logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewOpenTelemetry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestShutdownOpenTelemetryComponents(t *testing.T) {
	t.Run("All functions succeed", func(t *testing.T) {
		ctx := context.Background()

		shutdownFunc1 := func(ctx context.Context) error {
			return nil
		}
		shutdownFunc2 := func(ctx context.Context) error {
			return nil
		}

		shutdownFuncs := []func(context.Context) error{
			shutdownFunc1,
			shutdownFunc2,
		}

		shutdown := shutdownOpenTelemetryComponents(shutdownFuncs)
		err := shutdown(ctx)
		assert.NoError(t, err)
	})

	t.Run("One function fails", func(t *testing.T) {
		ctx := context.Background()

		shutdownFunc1 := func(ctx context.Context) error {
			return nil
		}
		shutdownFunc2 := func(ctx context.Context) error {
			return errors.New("shutdown error")
		}

		shutdownFuncs := []func(context.Context) error{
			shutdownFunc1,
			shutdownFunc2,
		}

		shutdown := shutdownOpenTelemetryComponents(shutdownFuncs)
		err := shutdown(ctx)
		assert.Error(t, err)
		assert.Equal(t, "shutdown error", err.Error())
	})

	t.Run("Multiple functions fail", func(t *testing.T) {
		ctx := context.Background()

		shutdownFunc1 := func(ctx context.Context) error {
			return errors.New("first shutdown error")
		}
		shutdownFunc2 := func(ctx context.Context) error {
			return errors.New("second shutdown error")
		}

		shutdownFuncs := []func(context.Context) error{
			shutdownFunc1,
			shutdownFunc2,
		}

		shutdown := shutdownOpenTelemetryComponents(shutdownFuncs)

		err := shutdown(ctx)
		assert.Error(t, err)
		assert.Equal(t, "second shutdown error", err.Error())
	})
}

func TestSRecordLatency(t *testing.T) {
	cyx := context.Background()
	srv := setupTestEndpoint(":7061", "/TestSRecordLatency")
	time.Sleep(3 * time.Second)
	defer func() {
		assert.NoError(t, srv.Shutdown(cyx), "failed to shutdown srv")
	}()

	var ds1 []func(context.Context) error

	cfg := &OTelConfig{
		TracerEndpoint:   "http://localhost:7060",
		MetricEndpoint:   "http://localhost:7061",
		ServiceName:      "test",
		OTELEnabled:      true,
		TraceSampleRatio: 20,
		Database:         "testDB",
		Instance:         "testInstance",
		HealthCheckEp:    "localhost:7061/TestSRecordLatency",
	}

	ot, ds, err := NewOpenTelemetry(cyx, cfg, nil)
	ds1 = append(ds1, ds)
	assert.NoErrorf(t, err, "error occurred")

	ot.RecordLatencyMetric(cyx, time.Now(), Attributes{Method: "handlePrepare"})
	assert.NoErrorf(t, err, "error occurred")
	//when otel is disabled
	cfg2 := &OTelConfig{
		OTELEnabled: false,
	}

	ot1, ds, err2 := NewOpenTelemetry(cyx, cfg2, nil)
	ds1 = append(ds1, ds)
	assert.NoErrorf(t, err, "error occurred")

	ot1.RecordLatencyMetric(cyx, time.Now(), Attributes{Method: "handlePrepare"})

	shutdownOpenTelemetryComponents(ds1)
	assert.NoErrorf(t, err2, "error occurred")
}

func TestRecordRequestCountMetric(t *testing.T) {
	cyx := context.Background()
	srv := setupTestEndpoint(":7061", "/TestRecordRequestCountMetric")
	time.Sleep(3 * time.Second)
	defer func() {
		assert.NoError(t, srv.Shutdown(cyx), "failed to shutdown srv")
	}()

	var ds1 []func(context.Context) error

	cfg := &OTelConfig{
		TracerEndpoint:   "http://localhost:7060",
		MetricEndpoint:   "http://localhost:7061",
		ServiceName:      "test",
		OTELEnabled:      true,
		TraceSampleRatio: 20,
		Database:         "testDB",
		Instance:         "testInstance",
		HealthCheckEp:    "localhost:7061/TestRecordRequestCountMetric",
	}

	ot, ds, err := NewOpenTelemetry(cyx, cfg, nil)
	ds1 = append(ds1, ds)
	assert.NoErrorf(t, err, "error occurred")

	ot.RecordRequestCountMetric(cyx, Attributes{Method: "handlePrepare"})

	assert.NoErrorf(t, err, "error occurred")

	//when otel is disabled
	cfg2 := &OTelConfig{
		OTELEnabled: false,
	}

	ot1, ds, err2 := NewOpenTelemetry(cyx, cfg2, nil)
	ds1 = append(ds1, ds)
	assert.NoErrorf(t, err, "error occurred")

	ot1.RecordRequestCountMetric(cyx, Attributes{Method: "handlePrepare"})

	shutdownOpenTelemetryComponents(ds1)
	assert.NoErrorf(t, err2, "error occurred")
}

func TestApplyTrace(t *testing.T) {
	cyx := context.Background()
	srv := setupTestEndpoint(":7061", "/TestApplyTrace")
	time.Sleep(3 * time.Second)
	defer func() {
		assert.NoError(t, srv.Shutdown(cyx), "failed to shutdown srv")
	}()

	var ds1 []func(context.Context) error

	cfg := &OTelConfig{
		TracerEndpoint:   "http://localhost:7060",
		MetricEndpoint:   "http://localhost:7061",
		ServiceName:      "test",
		OTELEnabled:      true,
		TraceSampleRatio: 20,
		Database:         "testDB",
		Instance:         "testInstance",
		HealthCheckEp:    "localhost:7061/TestApplyTrace",
	}

	ot, ds, err := NewOpenTelemetry(cyx, cfg, nil)
	ds1 = append(ds1, ds)
	assert.NoErrorf(t, err, "error occurred")

	_, span := ot.StartSpan(cyx, "test", []attribute.KeyValue{
		attribute.String("method", "handlePrepare"),
	})

	shutdownOpenTelemetryComponents(ds1)
	assert.NoErrorf(t, err, "error occurred")
	ot.EndSpan(span)
}

func TestShutdown(t *testing.T) {
	cyx := context.Background()
	srv := setupTestEndpoint(":7061", "/TestShutdown")
	time.Sleep(3 * time.Second)
	defer func() {
		assert.NoError(t, srv.Shutdown(cyx), "failed to shutdown srv")
	}()

	var ds1 []func(context.Context) error

	cfg := &OTelConfig{
		TracerEndpoint:   "http://localhost:7060",
		MetricEndpoint:   "http://localhost:7061",
		ServiceName:      "test",
		OTELEnabled:      true,
		TraceSampleRatio: 20,
		Database:         "testDB",
		Instance:         "testInstance",
		HealthCheckEp:    "localhost:7061/TestShutdown",
	}

	_, ds, err := NewOpenTelemetry(cyx, cfg, nil)
	ds1 = append(ds1, ds)
	assert.NoErrorf(t, err, "error occurred")
	shutdownOpenTelemetryComponents(ds1)
}

// testHandler handles requests to the test endpoint.
func testHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello, this is a test endpoint!")
}

func setupTestEndpoint(st string, typ string) *http.Server {
	// Create a new instance of a server
	server := &http.Server{Addr: st}

	// Register the test handler with the specific type
	http.HandleFunc(typ, testHandler)

	// Start the server in a goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("ListenAndServe error: %s\n", err)
		}
	}()

	return server
}

func TestSRecordError(t *testing.T) {
	cyx := context.Background()
	srv := setupTestEndpoint(":7061", "/TestSRecordError")
	time.Sleep(3 * time.Second)
	defer func() {
		assert.NoError(t, srv.Shutdown(cyx), "failed to shutdown srv")
	}()

	var ds1 []func(context.Context) error

	cfg := &OTelConfig{
		TracerEndpoint:   "http://localhost:7060",
		MetricEndpoint:   "http://localhost:7061",
		ServiceName:      "test",
		OTELEnabled:      true,
		TraceSampleRatio: 20,
		Database:         "testDB",
		Instance:         "testInstance",
		HealthCheckEp:    "localhost:7061/TestSRecordError",
	}

	ot, ds, err := NewOpenTelemetry(cyx, cfg, nil)
	ds1 = append(ds1, ds)
	assert.NoErrorf(t, err, "error occurred")

	_, span := ot.StartSpan(cyx, "test", []attribute.KeyValue{
		attribute.String("method", "handlePrepare"),
	})

	ot.RecordError(span, fmt.Errorf("test error"))

	shutdownOpenTelemetryComponents(ds1)
	assert.NoErrorf(t, err, "error occurred")
}
