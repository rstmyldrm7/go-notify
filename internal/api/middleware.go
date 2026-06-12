package api

import (
	"log/slog"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/rstmyldrm7/go-notify/internal/ctxutil"
	"github.com/rstmyldrm7/go-notify/internal/metrics"
)

const correlationIDHeader = "X-Correlation-ID"

// CorrelationID accepts the caller's correlation id or generates one, then
// propagates it via context (logs, Kafka headers) and the response.
func CorrelationID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(correlationIDHeader)
		if id == "" {
			id = uuid.NewString()
		}
		c.Writer.Header().Set(correlationIDHeader, id)
		c.Request = c.Request.WithContext(
			ctxutil.WithCorrelationID(c.Request.Context(), id))
		c.Next()
	}
}

// Metrics records request count, latency and in-flight gauge for every
// request. It labels by the matched route template (c.FullPath()) rather than
// the raw URL so that per-id paths do not blow up label cardinality.
func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		metrics.HTTPInFlight.Inc()
		start := time.Now()

		c.Next()

		metrics.HTTPInFlight.Dec()
		route := c.FullPath()
		if route == "" {
			route = "unmatched" // 404s on routes Gin never matched
		}
		method := c.Request.Method
		metrics.HTTPRequests.WithLabelValues(method, route, strconv.Itoa(c.Writer.Status())).Inc()
		metrics.HTTPDuration.WithLabelValues(method, route).Observe(time.Since(start).Seconds())
	}
}

// RequestLogger emits one structured log line per request.
func RequestLogger(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.InfoContext(c.Request.Context(), "http request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
			"correlation_id", ctxutil.CorrelationID(c.Request.Context()),
		)
	}
}
