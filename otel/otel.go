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
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type Attributes struct {
	Method    string
	Status    string
	QueryType string
}

var (
	attributeKeyDatabase  = attribute.Key("database")
	attributeKeyMethod    = attribute.Key("method")
	attributeKeyStatus    = attribute.Key("status")
	attributeKeyInstance  = attribute.Key("instance")
	attributeKeyQueryType = attribute.Key("query_type")
)

// TracerProvider defines the interface for creating traces.
type TracerProvider interface {
	InitTracerProvider(ctx context.Context) (*sdktrace.TracerProvider, error)
}

// MeterProvider defines the interface for creating meters.
type MeterProvider interface {
	InitMeterProvider(ctx context.Context) (*sdkmetric.MeterProvider, error)
}

// TelemetryInitializer defines the interface for initializing OpenTelemetry components.
type TelemetryInitializer interface {
	InitOpenTelemetry(ctx context.Context) (shutdown func(context.Context) error, err error)
}

// OTelConfig holds configuration for OpenTelemetry.
type OTelConfig struct {
	TraceEnabled         bool
	MetricEnabled        bool
	TracerEndpoint       string
	MetricEndpoint       string
	ServiceName          string
	TraceSampleRatio     float64
	OTELEnabled          bool
	Database             string
	Instance             string
	HealthCheckEnabled   bool
	HealthCheckEp        string
	ServiceVersion       string
	ServiceInstanceIDKey string
}

const (
	requestCountMetric = "spanner/cassandra_adapter/request_count"
	latencyMetric      = "spanner/cassandra_adapter/roundtrip_latencies"
)

// OpenTelemetry provides methods to setup tracing and metrics.
type OpenTelemetry struct {
	Config         *OTelConfig
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *sdkmetric.MeterProvider
	Tracer         trace.Tracer
	Meter          metric.Meter
	requestCount   metric.Int64Counter   // Default noop
	requestLatency metric.Int64Histogram // Default noop
	Logger         *zap.Logger
	attributeMap   []attribute.KeyValue
}

// NewOpenTelemetry creates and initializes a new instance of OpenTelemetry, including
// its Tracer and Meter providers, and returns Tracer and Meter instances.
func NewOpenTelemetry(ctx context.Context, config *OTelConfig, logger *zap.Logger) (*OpenTelemetry, func(context.Context) error, error) {
	otelInst := &OpenTelemetry{Config: config, Logger: logger, attributeMap: []attribute.KeyValue{}}
	var err error
	otelInst.Config.OTELEnabled = config.OTELEnabled
	if !config.OTELEnabled {
		otelInst.Config.OTELEnabled = config.OTELEnabled
		return otelInst, nil, nil
	}

	// Construct attributes for Metrics
	attributeMap := []attribute.KeyValue{
		attributeKeyInstance.String(config.Instance),
		attributeKeyDatabase.String(config.Database),
	}
	otelInst.attributeMap = append(otelInst.attributeMap, attributeMap...)

	if config.HealthCheckEnabled {
		resp, err := http.Get("http://" + config.HealthCheckEp)
		if err != nil {
			return otelInst, nil, err
		}
		if resp.StatusCode != 200 {
			return otelInst, nil, errors.New("OTEL collector service is not up and running")
		}
		logger.Info("OTEL health check COMPLETE")
	}
	var shutdownFuncs []func(context.Context) error
	resource := otelInst.createResource(ctx)

	if config.TraceEnabled {
		// Initialize TracerProvider
		otelInst.TracerProvider, err = otelInst.InitTracerProvider(ctx, resource)
		if err != nil {
			logger.Error("error while initializing the tracer provider", zap.Error(err))
			return nil, nil, err
		}
		otel.SetTracerProvider(otelInst.TracerProvider)
		shutdownFuncs = append(shutdownFuncs, otelInst.TracerProvider.Shutdown)
	}
	otelInst.Tracer = otel.GetTracerProvider().Tracer(config.ServiceName)

	if config.MetricEnabled {
		// Initialize MeterProvider
		otelInst.MeterProvider, err = otelInst.InitMeterProvider(ctx, resource)
		if err != nil {
			logger.Error("error while initializing the meter provider", zap.Error(err))
			return nil, nil, err
		}
		otel.SetMeterProvider(otelInst.MeterProvider)
		otelInst.Meter = otelInst.MeterProvider.Meter(config.ServiceName)
		shutdownFuncs = append(shutdownFuncs, otelInst.MeterProvider.Shutdown)

	} else {
		otelInst.Meter = otel.GetMeterProvider().Meter(config.ServiceName)
	}
	shutdown := shutdownOpenTelemetryComponents(shutdownFuncs)
	otelInst.requestCount, err = otelInst.Meter.Int64Counter(requestCountMetric, metric.WithDescription("Records metric for number of query requests coming in"), metric.WithUnit("1"))
	if err != nil {
		logger.Error("error during registering instrument for metric spanner/cassandra_adapter/request_count", zap.Error(err))
		return otelInst, shutdown, err
	}
	otelInst.requestLatency, err = otelInst.Meter.Int64Histogram(latencyMetric,
		metric.WithDescription("Records latency for all query operations"),
		metric.WithExplicitBucketBoundaries(0.0, 0.0010, 0.0013, 0.0016, 0.0020, 0.0024, 0.0031, 0.0038, 0.0048, 0.0060,
			0.0075, 0.0093, 0.0116, 0.0146, 0.0182, 0.0227, 0.0284, 0.0355, 0.0444, 0.0555, 0.0694, 0.0867,
			0.1084, 0.1355, 0.1694, 0.2118, 0.2647, 0.3309, 0.4136, 0.5170, 0.6462, 0.8078, 1.0097, 1.2622,
			1.5777, 1.9722, 2.4652, 3.0815, 3.8519, 4.8148, 6.0185, 7.5232, 9.4040, 11.7549, 14.6937, 18.3671,
			22.9589, 28.6986, 35.8732, 44.8416, 56.0519, 70.0649, 87.5812, 109.4764, 136.8456, 171.0569, 213.8212,
			267.2765, 334.0956, 417.6195, 522.0244, 652.5304),
		metric.WithUnit("ms"))
	if err != nil {
		logger.Error("error during registering instrument for metric spanner/cassandra_adapter/roundtrip_latencies", zap.Error(err))
		return otelInst, shutdown, err
	}
	return otelInst, shutdown, nil
}

// shutdownOpenTelemetryComponents cleanly shuts down all OpenTelemetry components initialized.
func shutdownOpenTelemetryComponents(shutdownFuncs []func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		var shutdownErr error
		for _, shutdownFunc := range shutdownFuncs {
			if err := shutdownFunc(ctx); err != nil {
				shutdownErr = err
			}
		}
		return shutdownErr
	}
}

// InitTracerProvider initializes the TracerProvider for OpenTelemetry. This function
// configures a gRPC exporter for trace data, pointing to the configured TracerEndpoint.
// It returns an initialized TracerProvider or an error if the initialization fails.
func (o *OpenTelemetry) InitTracerProvider(ctx context.Context, resource *resource.Resource) (*sdktrace.TracerProvider, error) {
	sampler := sdktrace.TraceIDRatioBased(o.Config.TraceSampleRatio)
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(o.Config.TracerEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		o.Logger.Error("error while initializing the tracer", zap.Error(err))
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(resource),
		sdktrace.WithSampler(sdktrace.ParentBased(sampler)),
	)
	return tp, nil
}

// InitMeterProvider initializes the MeterProvider for OpenTelemetry. This function sets up a gRPC exporter for metrics data,
// targeting the configured MetricEndpoint.
// It returns an initialized MeterProvider or an error if the setup fails. The MeterProvider is responsible for collecting and
// exporting metrics from your application to an OpenTelemetry Collector or directly to a backend that supports OTLP over gRPC for metrics.
func (o *OpenTelemetry) InitMeterProvider(ctx context.Context, resource *resource.Resource) (*sdkmetric.MeterProvider, error) {
	var views []sdkmetric.View
	me, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(o.Config.MetricEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		o.Logger.Error("error while initializing the meter", zap.Error(err))
		return nil, err
	}

	// Define views to filter out unwanted gRPC metrics
	views = []sdkmetric.View{
		sdkmetric.NewView(
			sdkmetric.Instrument{Name: "rpc.client.*"},                 // Wildcard pattern to match gRPC client metrics
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationDrop{}}, // Drop these metrics
		)}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(me)),
		sdkmetric.WithResource(resource),
		sdkmetric.WithView(views...),
	)
	return mp, nil
}

// Function to create otel resource.
func (o *OpenTelemetry) createResource(ctx context.Context) *resource.Resource {
	serviceInstanceIdKey := uuid.New().String()
	if o.Config.ServiceInstanceIDKey != "" {
		serviceInstanceIdKey = o.Config.ServiceInstanceIDKey
	}
	res, err := resource.New(ctx,
		resource.WithSchemaURL(semconv.SchemaURL),
		// Use the GCP resource detector!
		resource.WithDetectors(gcp.NewDetector()),
		// Keep the default detectors
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(o.Config.ServiceName),
			semconv.ServiceInstanceIDKey.String(serviceInstanceIdKey),
			semconv.ServiceVersionKey.String(o.Config.ServiceVersion),
		),
	)

	if err != nil {
		// Default resource
		return resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(o.Config.ServiceName),
			semconv.ServiceInstanceIDKey.String(serviceInstanceIdKey),
			semconv.ServiceVersionKey.String(o.Config.ServiceVersion),
		)
	}

	return res

}

// CreateTrace starts a new trace span based on provided context, name, attributes, and error.
// It returns a new context containing the span.
func (o *OpenTelemetry) StartSpan(ctx context.Context, name string, attrs []attribute.KeyValue) (context.Context, trace.Span) {
	if !o.Config.OTELEnabled || !o.Config.TraceEnabled {
		return ctx, nil
	}

	ctx, span := o.Tracer.Start(ctx, name, trace.WithAttributes(attrs...))
	return ctx, span
}

// RecordError records a new error under a span.
func (o *OpenTelemetry) RecordError(span trace.Span, err error) {
	if !o.Config.OTELEnabled || !o.Config.TraceEnabled {
		return
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
}

// SetError records an error for the span retrieved from the provided context. If OpenTelemetry (OTEL) is not enabled
// or if the error is nil, the function will return immediately without recording the error. If enabled and an error
// is present, the error will be recorded and the span status will be set to error with the corresponding error message.
//
// Parameters:
// - ctx: The context from which the span is retrieved.
// - err: The error to be recorded and set on the span.
func (o *OpenTelemetry) SetError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	if o.Config.OTELEnabled && o.Config.TraceEnabled && err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// EndSpan stops the span.
func (o *OpenTelemetry) EndSpan(span trace.Span) {
	if !o.Config.OTELEnabled || !o.Config.TraceEnabled {
		return
	}

	span.End()
}

// RecordLatencyMetric adds the latency metric based on provided context, name, duration and attributes.
func (o *OpenTelemetry) RecordLatencyMetric(ctx context.Context, duration time.Time, attrs Attributes) {
	if !o.Config.OTELEnabled || !o.Config.MetricEnabled {
		return
	}

	attr := o.attributeMap
	attr = append(attr, attributeKeyMethod.String(attrs.Method))
	attr = append(attr, attributeKeyQueryType.String(attrs.QueryType))
	o.requestLatency.Record(ctx, int64(time.Since(duration).Milliseconds()), metric.WithAttributes(attr...))
}

// RecordRequestCountMetric adds the request count based on provided context, name and attributes.
func (o *OpenTelemetry) RecordRequestCountMetric(ctx context.Context, attrs Attributes) {
	if !o.Config.OTELEnabled || !o.Config.MetricEnabled {
		return
	}

	attr := o.attributeMap
	attr = append(attr, attributeKeyMethod.String(attrs.Method))
	attr = append(attr, attributeKeyQueryType.String(attrs.QueryType))
	attr = append(attr, attributeKeyStatus.String(attrs.Status))
	o.requestCount.Add(ctx, 1, metric.WithAttributes(attr...))
}

// AddAnnotation add event to the span of the given ctx.
func AddAnnotation(ctx context.Context, event string) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(event)
}

// AddAnnotationWithAttr add event to the span of the given ctx with the necessary attributes.
func AddAnnotationWithAttr(ctx context.Context, event string, attr []attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(event, trace.WithAttributes(attr...))
}
