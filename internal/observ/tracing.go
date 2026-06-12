// Package observ holds the OpenTelemetry tracing bootstrap shared by every
// service. One InitTracer call per binary wires an OTLP/gRPC exporter to Tempo
// and installs the W3C TraceContext propagator so a single trace flows across
// the API → Kafka → worker → provider hops.
//
// Tracing is opt-in: when the OTLP endpoint is empty (local runs, tests) the
// global tracer stays a no-op and the services behave exactly as before.
package observ

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Tracer is the package-wide tracer name used for manually created spans
// (Kafka publish/consume, worker processing). It identifies the
// instrumentation, not the service.
const Tracer = "github.com/rstmyldrm7/go-notify"

// InitTracer configures the global TracerProvider and propagator. The returned
// shutdown flushes any buffered spans and must be deferred by the caller.
//
// When endpoint is empty InitTracer installs only the propagator (so inbound
// trace headers still parse) and returns a no-op shutdown: spans become no-ops
// and nothing is exported.
func InitTracer(ctx context.Context, serviceName, endpoint string, log *slog.Logger) (func(context.Context) error, error) {
	// The propagator is always set so cross-service header parsing is
	// consistent whether or not this binary exports spans.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if endpoint == "" {
		log.Info("tracing disabled (no OTLP endpoint configured)")
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		// Local Tempo speaks plaintext gRPC; no TLS to terminate.
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	log.Info("tracing enabled", "service", serviceName, "otlp_endpoint", endpoint)

	return tp.Shutdown, nil
}

// FlushOnShutdown runs a tracer shutdown with a bounded timeout. It is meant to
// be deferred in each service's run loop so buffered spans are flushed on exit.
func FlushOnShutdown(shutdown func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = shutdown(ctx)
}

// TraceID returns the active trace id for ctx, or "" when there is no recording
// span. It is logged alongside the correlation id so a log line can be pivoted
// to its trace in Grafana.
func TraceID(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}
