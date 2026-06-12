// Command mockprovider is a self-contained stand-in for the external
// notification provider (the assessment suggests webhook.site). It accepts the
// provider request shape and replies with the documented 202 acknowledgement,
// so the whole system can run end to end from `docker compose up` with no
// external dependency.
//
// Set MOCK_FAIL_RATE (0..1) to randomly return 503s and exercise the worker's
// in-memory retry and DLQ paths.
package main

import (
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

func main() {
	addr := getenv("MOCK_ADDR", ":8080")
	failRate := getfloat("MOCK_FAIL_RATE", 0)

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			To      string `json:"to"`
			Channel string `json:"channel"`
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		if failRate > 0 && rand.Float64() < failRate {
			log.Warn("simulated provider failure", "to", req.To, "channel", req.Channel)
			http.Error(w, "simulated upstream error", http.StatusServiceUnavailable)
			return
		}

		log.Info("accepted", "to", req.To, "channel", req.Channel)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"messageId": uuid.NewString(),
			"status":    "accepted",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	})

	log.Info("mock provider listening", "addr", addr, "fail_rate", failRate)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		log.Error("mock provider stopped", "error", err)
		os.Exit(1)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getfloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}
