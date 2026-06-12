// Package metrics defines every Prometheus collector in the system and the
// handler that serves them. All three services share this one package and the
// common notify_* namespace; each binary serves its own /metrics, so a given
// target only carries non-zero values for the metrics that service actually
// updates.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// --- API (ingestion) metrics ---

var (
	// HTTPRequests counts handled requests. The route label is the matched Gin
	// route template (e.g. /api/v1/notifications/:id), never the raw path, so
	// per-id URLs do not blow up label cardinality.
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notify_api_http_requests_total",
		Help: "HTTP requests handled, by method, route template and status code.",
	}, []string{"method", "route", "status"})

	// HTTPDuration tracks request latency in seconds, by method and route.
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "notify_api_http_request_duration_seconds",
		Help:    "HTTP request latency in seconds, by method and route template.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})

	// HTTPInFlight reports the number of requests currently being served.
	HTTPInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "notify_api_http_in_flight_requests",
		Help: "HTTP requests currently being served.",
	})

	// NotificationsCreated counts notifications persisted by the API (fresh
	// inserts only; idempotent replays are excluded), by channel and priority.
	NotificationsCreated = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notify_api_notifications_created_total",
		Help: "Notifications persisted by the API, by channel and priority.",
	}, []string{"channel", "priority"})

	// NotificationsPublished counts publish attempts to Kafka by result
	// ("success" or "failure"). A rising failure rate means rows are stranded
	// in 'pending' and surfaces the dual-write gap that monitoring should alert
	// on.
	NotificationsPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notify_api_notifications_published_total",
		Help: "Notifications published to Kafka by the API, by result.",
	}, []string{"result"})
)

// --- Worker (delivery) metrics ---

var (
	// MessagesProcessed counts terminal outcomes per channel/priority. The
	// result label is "sent" or "dead" (offloaded to the DLQ).
	MessagesProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notify_worker_messages_processed_total",
		Help: "Notifications reaching a terminal state, by channel, priority and result.",
	}, []string{"channel", "priority", "result"})

	// ProviderAttempts counts individual provider HTTP attempts and their
	// outcome ("success", "retryable_error", "permanent_error").
	ProviderAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notify_worker_provider_attempts_total",
		Help: "External provider HTTP attempts, by channel and outcome.",
	}, []string{"channel", "outcome"})

	// DeliveryDuration tracks provider call latency in seconds, per channel.
	DeliveryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "notify_worker_delivery_seconds",
		Help:    "External provider delivery latency in seconds, by channel.",
		Buckets: prometheus.DefBuckets,
	}, []string{"channel"})

	// QueueDepth reports the live depth of each in-memory priority channel.
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "notify_worker_inmemory_queue_depth",
		Help: "Current depth of the in-memory priority channels, by channel and priority.",
	}, []string{"channel", "priority"})

	// Inflight reports how many messages are currently being delivered.
	Inflight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "notify_worker_inflight",
		Help: "Messages currently in delivery, by channel.",
	}, []string{"channel"})
)

// --- Scheduler metrics ---

var (
	// SchedulerDispatched counts scheduled notifications published to Kafka.
	SchedulerDispatched = promauto.NewCounter(prometheus.CounterOpts{
		Name: "notify_scheduler_dispatched_total",
		Help: "Scheduled notifications dispatched to Kafka.",
	})

	// SchedulerDispatchErrors counts ticks that errored before committing.
	SchedulerDispatchErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "notify_scheduler_dispatch_errors_total",
		Help: "Failed dispatch attempts (errored before committing the batch).",
	})

	// SchedulingLag measures how late a notification was dispatched relative to
	// its scheduled_at. A rising lag means the poller is falling behind.
	SchedulingLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "notify_scheduler_scheduling_lag_seconds",
		Help:    "Delay between a notification's scheduled_at and its dispatch.",
		Buckets: []float64{0.5, 1, 2, 5, 10, 30, 60, 120, 300},
	})
)

// Handler returns the Prometheus scrape handler for /metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}

// StartObservabilityServer starts an HTTP server exposing /metrics (Prometheus)
// and /healthz (delegating to healthy) and returns it so the caller can shut it
// down. Shared by the worker and scheduler so both expose the same surface.
func StartObservabilityServer(addr string, healthy func(context.Context) error, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := healthy(r.Context()); err != nil {
			http.Error(w, `{"status":"degraded","database":"unreachable"}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("observability server failed", "addr", addr, "error", err)
		}
	}()
	return srv
}
