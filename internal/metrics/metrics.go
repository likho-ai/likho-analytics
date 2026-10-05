// Package metrics is what the service tells about itself (OpenTelemetry): always as Prometheus
// text at GET /metrics, and pushed over OTLP/HTTP as well when an endpoint is set.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Metrics holds the instruments the service records into.
type Metrics struct {
	provider *sdkmetric.MeterProvider
	registry *prometheus.Registry

	// gRPC calls answered, by method and status.
	Requests metric.Int64Counter
	// How long a call took, in seconds, by method.
	RequestSeconds metric.Float64Histogram
	// Events written to ClickHouse, by type.
	EventsStored metric.Int64Counter
	// Events taken from the bus, by subject and outcome.
	EventsHandled metric.Int64Counter
	// How long a question to ClickHouse took, in seconds, by query.
	QuerySeconds metric.Float64Histogram
	// 1 while the service runs (so /metrics is never empty).
	up metric.Int64ObservableGauge
}

// New builds the instruments. otlpEndpoint empty = only /metrics.
func New(ctx context.Context, service, version, otlpEndpoint string) (*Metrics, error) {
	registry := prometheus.NewRegistry()
	promReader, err := otelprom.New(otelprom.WithRegisterer(registry), otelprom.WithoutScopeInfo())
	if err != nil {
		return nil, fmt.Errorf("metrics: %w", err)
	}
	options := []sdkmetric.Option{
		sdkmetric.WithReader(promReader),
		sdkmetric.WithResource(resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(service), semconv.ServiceVersion(version))),
	}
	if otlpEndpoint != "" {
		exporter, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpointURL(strings.TrimRight(otlpEndpoint, "/")+"/v1/metrics"))
		if err != nil {
			return nil, fmt.Errorf("metrics: %w", err)
		}
		options = append(options, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter,
			sdkmetric.WithInterval(15*time.Second))))
	}
	provider := sdkmetric.NewMeterProvider(options...)
	meter := provider.Meter(service)

	m := &Metrics{provider: provider, registry: registry}
	if m.Requests, err = meter.Int64Counter("likho_analytics_requests",
		metric.WithDescription("gRPC calls answered, by method and status")); err != nil {
		return nil, err
	}
	if m.RequestSeconds, err = meter.Float64Histogram("likho_analytics_request_seconds",
		metric.WithDescription("How long a call took"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)); err != nil {
		return nil, err
	}
	if m.EventsStored, err = meter.Int64Counter("likho_analytics_events_stored",
		metric.WithDescription("Events written to ClickHouse, by type")); err != nil {
		return nil, err
	}
	if m.EventsHandled, err = meter.Int64Counter("likho_analytics_events_handled",
		metric.WithDescription("Events taken from the bus, by subject and outcome")); err != nil {
		return nil, err
	}
	if m.QuerySeconds, err = meter.Float64Histogram("likho_analytics_query_seconds",
		metric.WithDescription("How long a question to ClickHouse took, by query"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)); err != nil {
		return nil, err
	}
	if m.up, err = meter.Int64ObservableGauge("likho_analytics_up",
		metric.WithDescription("1 while the service runs")); err != nil {
		return nil, err
	}
	if _, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(m.up, 1)
		return nil
	}, m.up); err != nil {
		return nil, err
	}
	return m, nil
}

// Handler serves the Prometheus text.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Timed wraps an HTTP handler (the gRPC mux) so every call is counted and timed by method.
func (m *Metrics) Timed(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		attrs := metric.WithAttributes(attribute.String("method", method),
			attribute.String("status", fmt.Sprint(recorder.status)))
		m.Requests.Add(r.Context(), 1, attrs)
		m.RequestSeconds.Record(r.Context(), time.Since(started).Seconds(),
			metric.WithAttributes(attribute.String("method", method)))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Flush lets the response stream (gRPC over HTTP/2 needs it).
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Outcome is the attribute set for an outcome label.
func Outcome(value string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("outcome", value))
}

// Subject is the attribute set for a bus subject.
func Subject(value string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("subject", value))
}

// Type is the attribute set for an event type.
func Type(value string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("type", value))
}

// Query is the attribute set for a query name.
func Query(value string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("query", value))
}

// Close flushes what is pending to the OTLP endpoint, if any.
func (m *Metrics) Close(ctx context.Context) error {
	return m.provider.Shutdown(ctx)
}
