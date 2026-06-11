package api

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/rstmyldrm7/go-notify/internal/ctxutil"
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
