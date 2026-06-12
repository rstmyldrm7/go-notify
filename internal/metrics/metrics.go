// Package metrics defines the Prometheus collectors exported by the worker and
// the HTTP handler that serves them. It satisfies the observability requirement
// (queue depth, success/failure rates, delivery latency).
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

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
