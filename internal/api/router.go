package api

import (
	"log/slog"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"github.com/rstmyldrm7/go-notify/internal/metrics"
)

func NewRouter(h *Handler, log *slog.Logger) *gin.Engine {
	r := gin.New()
	// otelgin first so the server span wraps the whole chain and downstream
	// middleware/handlers see the trace context. Metrics next so it times the
	// full chain, including panics that Recovery turns into 500s.
	//
	// Skip tracing the operational endpoints: Prometheus scrapes /metrics every
	// few seconds and liveness probes hit /healthz, so tracing them would bury
	// real request traces under constant noise in Tempo.
	r.Use(
		otelgin.Middleware("notify-api", otelgin.WithGinFilter(traceable)),
		Metrics(), gin.Recovery(), CorrelationID(), RequestLogger(log),
	)

	r.GET("/healthz", h.Healthz)
	r.GET("/metrics", gin.WrapH(metrics.Handler()))
	registerDocs(r)

	v1 := r.Group("/api/v1")
	{
		v1.POST("/notifications", h.CreateNotification)
		v1.POST("/notifications/batch", h.CreateBatch)
		v1.GET("/notifications", h.ListNotifications)
		v1.GET("/notifications/:id", h.GetNotification)
		v1.DELETE("/notifications/:id", h.CancelNotification)
		v1.GET("/batches/:id", h.GetBatch)
	}
	return r
}

// traceable reports whether a request should be traced. Operational endpoints
// (metrics scrapes, health probes) are excluded so they don't flood Tempo.
func traceable(c *gin.Context) bool {
	switch c.Request.URL.Path {
	case "/metrics", "/healthz":
		return false
	default:
		return true
	}
}
