package obs

import (
	"context"
	"net/url"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation scope used by this service.
const TracerName = "github.com/Matthew-Klette/inference-queue"

// Tracer returns the service tracer. Until SetupTracing installs a provider it
// is the global no-op tracer, so call sites never need a nil check.
func Tracer() trace.Tracer { return otel.Tracer(TracerName) }

// SetupTracing installs an OTLP/HTTP exporter when endpoint is non-empty.
// With an empty endpoint nothing is installed and tracing stays a no-op. The
// returned function flushes and stops the provider.
func SetupTracing(ctx context.Context, service, endpoint string) (func(context.Context) error, error) {
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	opts := []otlptracehttp.Option{}
	if u, err := url.Parse(endpoint); err == nil && u.Scheme != "" {
		opts = append(opts, otlptracehttp.WithEndpointURL(endpoint))
	} else {
		opts = append(opts, otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithInsecure())
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(service))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
