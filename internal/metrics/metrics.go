// Package metrics defines every Prometheus collector in the system and the
// handler that serves them. All three services share this one package and the
// common notify_* namespace; each binary serves its own /metrics, so a given
// target only carries non-zero values for the metrics that service actually
// updates.
package metrics

import (
	"net/http"

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

// Handler returns the Prometheus scrape handler for /metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}
