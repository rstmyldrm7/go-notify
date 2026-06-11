package config

import (
	"os"
	"strings"
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
}

func Load() Config {
	return Config{
		HTTPAddr:       getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:    getenv("DATABASE_URL", "postgres://notify:notify@localhost:5432/notify?sslmode=disable"),
		KafkaBrokers:   strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ","),
		Env:            getenv("APP_ENV", "development"),
		MigrateOnStart: getenv("MIGRATE_ON_START", "true") == "true",
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
