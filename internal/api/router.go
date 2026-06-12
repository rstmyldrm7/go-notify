package api

import (
	"log/slog"

	"github.com/gin-gonic/gin"

	"github.com/rstmyldrm7/go-notify/internal/metrics"
)

func NewRouter(h *Handler, log *slog.Logger) *gin.Engine {
	r := gin.New()
	// Metrics first so it times the full chain, including panics that Recovery
	// turns into 500s.
	r.Use(Metrics(), gin.Recovery(), CorrelationID(), RequestLogger(log))

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
