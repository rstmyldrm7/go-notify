package api

import (
	_ "embed"
	"net/http"

	"github.com/gin-gonic/gin"
)

// openapiSpec is the hand-authored API contract, embedded so the binary serves
// its own docs with no files to ship alongside it.
//
//go:embed openapi.yaml
var openapiSpec []byte

// swaggerUI loads Swagger UI from a CDN and points it at /openapi.yaml. Keeping
// the assets on the CDN avoids vendoring the UI bundle; it needs outbound
// internet to render.
const swaggerUI = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>go-notify API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({ url: '/openapi.yaml', dom_id: '#swagger-ui' });
  </script>
</body>
</html>`

// registerDocs wires the spec and the interactive UI:
//
//	GET /openapi.yaml  -> the raw OpenAPI document
//	GET /docs          -> Swagger UI rendering that document
func registerDocs(r *gin.Engine) {
	r.GET("/openapi.yaml", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/yaml", openapiSpec)
	})
	r.GET("/docs", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(swaggerUI))
	})
}
