package api

import (
	"log/slog"

	"github.com/gin-gonic/gin"
)

func NewRouter(h *Handler, log *slog.Logger) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery(), CorrelationID(), RequestLogger(log))

	r.GET("/healthz", h.Healthz)

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
