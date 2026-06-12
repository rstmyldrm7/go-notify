package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// HTTPAddr is the listen address of the API server, e.g. ":8080".
	HTTPAddr string
	// DatabaseURL is the Postgres DSN shared by all services.
	DatabaseURL string
	// KafkaBrokers is the comma-separated broker list.
	KafkaBrokers []string
	// Env is "development" or "production"; controls log format and gin mode.
	Env string
	// MigrateOnStart runs pending SQL migrations during API startup so that
	// `docker compose up` works without a separate migrate step.
	MigrateOnStart bool

	// --- Worker / delivery ---

	// ConsumerGroupPrefix is combined with the channel to form a consumer
	// group per channel, e.g. "notify-worker-sms".
	ConsumerGroupPrefix string
	// ProviderURL is the external provider endpoint (your webhook.site URL).
	ProviderURL string
	// ProviderTimeout bounds a single delivery HTTP call.
	ProviderTimeout time.Duration
	// RateLimitPerSec caps deliveries per second per channel (spec: 100).
	RateLimitPerSec int
	// RateLimitBurst is the token bucket burst size; defaults to the rate.
	RateLimitBurst int
	// SenderConcurrency is the number of sender goroutines per channel pool.
	SenderConcurrency int
	// QueueBufferSize is the capacity of each in-memory priority channel.
	QueueBufferSize int
	// RetryMaxAttempts is the number of in-memory delivery attempts before DLQ.
	RetryMaxAttempts int
	// RetryBackoff is the base delay between in-memory retries (linear).
	RetryBackoff time.Duration
	// MetricsAddr is the listen address for the worker's /metrics and /healthz.
	MetricsAddr string

	// --- Scheduler ---

	// SchedulerInterval is how often the poller scans for due notifications.
	SchedulerInterval time.Duration
	// SchedulerBatchSize bounds how many due rows are claimed per transaction.
	SchedulerBatchSize int
}

func Load() Config {
	return Config{
		HTTPAddr:       getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:    getenv("DATABASE_URL", "postgres://notify:notify@localhost:5432/notify?sslmode=disable"),
		KafkaBrokers:   strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ","),
		Env:            getenv("APP_ENV", "development"),
		MigrateOnStart: getbool("MIGRATE_ON_START", true),

		ConsumerGroupPrefix: getenv("CONSUMER_GROUP_PREFIX", "notify-worker"),
		ProviderURL:         getenv("PROVIDER_URL", ""),
		ProviderTimeout:     getduration("PROVIDER_TIMEOUT", 10*time.Second),
		RateLimitPerSec:     getint("RATE_LIMIT_PER_SEC", 100),
		RateLimitBurst:      getint("RATE_LIMIT_BURST", 100),
		SenderConcurrency:   getint("SENDER_CONCURRENCY", 16),
		QueueBufferSize:     getint("QUEUE_BUFFER_SIZE", 256),
		RetryMaxAttempts:    getint("RETRY_MAX_ATTEMPTS", 3),
		RetryBackoff:        getduration("RETRY_BACKOFF", 500*time.Millisecond),
		MetricsAddr:         getenv("METRICS_ADDR", ":9100"),

		SchedulerInterval:  getduration("SCHEDULER_INTERVAL", 5*time.Second),
		SchedulerBatchSize: getint("SCHEDULER_BATCH_SIZE", 500),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getint(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getbool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}

func getduration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
