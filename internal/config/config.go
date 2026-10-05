// Package config reads the service's settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is everything that can be set from outside. The defaults match the likho-infra local stack.
type Config struct {
	Env      string
	LogLevel string

	HTTPPort int // /healthz, /readyz, /metrics
	GRPCPort int // likho.analytics.v1.AnalyticsService

	NATSURL string
	// NATSConnectTimeout is how long the start keeps trying to reach NATS before giving up.
	NATSConnectTimeout time.Duration
	// OTLPEndpoint is where metrics are pushed as well (OTLP/HTTP); empty = only GET /metrics.
	OTLPEndpoint string

	// ClickHouseURL is http[s]://user:password@host:8123/database.
	ClickHouseURL string
	// MigrateOnStart creates or updates the tables and views at start.
	MigrateOnStart bool
	// RetentionDays is how long events are kept.
	RetentionDays int
	// Location is the zone a day begins in (the workspace's): TIMEZONE, else TZ, else UTC.
	Location *time.Location

	// ConsumerGroup prefixes the names of the durable consumers (likho-analytics-<name>).
	ConsumerGroup string
	// ConsumersEnabled is false for an instance that only answers questions.
	ConsumersEnabled bool
	// ConsumerStart is where a consumer group new to the bus starts: "all" (every event the
	// stream still holds) or "new" (only from now on).
	ConsumerStart string
}

// Load reads the configuration and checks it.
func Load() (Config, error) {
	var problems []string
	number := func(name string, fallback int) int {
		raw, ok := os.LookupEnv(name)
		if !ok || raw == "" {
			return fallback
		}
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			problems = append(problems, fmt.Sprintf("%s must be a whole number, got %q", name, raw))
			return fallback
		}
		return value
	}
	text := func(name, fallback string) string {
		if value, ok := os.LookupEnv(name); ok && value != "" {
			return value
		}
		return fallback
	}
	flag := func(name string, fallback bool) bool {
		raw, ok := os.LookupEnv(name)
		if !ok || raw == "" {
			return fallback
		}
		switch strings.ToLower(raw) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
		problems = append(problems, fmt.Sprintf("%s must be true or false, got %q", name, raw))
		return fallback
	}

	zoneName := text("TIMEZONE", text("TZ", "UTC"))
	location, err := time.LoadLocation(zoneName)
	if err != nil {
		problems = append(problems, fmt.Sprintf("TIMEZONE %q is not a known zone", zoneName))
		location = time.UTC
	}

	cfg := Config{
		Env:                text("LIKHO_ENV", "development"),
		LogLevel:           text("LOG_LEVEL", "INFO"),
		HTTPPort:           number("HTTP_PORT", 4070),
		GRPCPort:           number("GRPC_PORT", 5070),
		NATSURL:            text("NATS_URL", "nats://localhost:4222"),
		NATSConnectTimeout: time.Duration(number("NATS_CONNECT_TIMEOUT_SECONDS", 120)) * time.Second,
		OTLPEndpoint:       text("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		ClickHouseURL:      text("CLICKHOUSE_URL", "http://likho_analytics:likho_analytics@localhost:8123/likho_analytics"),
		MigrateOnStart:     flag("MIGRATE_ON_START", true),
		RetentionDays:      number("RETENTION_DAYS", 400),
		Location:           location,
		ConsumerGroup:      text("CONSUMER_GROUP", "likho-analytics"),
		ConsumersEnabled:   flag("CONSUMERS_ENABLED", true),
		ConsumerStart:      strings.ToLower(text("CONSUMER_START", "all")),
	}
	if cfg.ConsumerStart != "all" && cfg.ConsumerStart != "new" {
		problems = append(problems, fmt.Sprintf("CONSUMER_START must be all or new, got %q", cfg.ConsumerStart))
	}
	if cfg.RetentionDays < 1 {
		problems = append(problems, "RETENTION_DAYS must be at least 1")
	}
	if (cfg.Env == "staging" || cfg.Env == "production") && strings.Contains(cfg.ClickHouseURL, ":likho_analytics@") {
		problems = append(problems, "CLICKHOUSE_URL must carry a real password in "+cfg.Env)
	}
	if len(problems) > 0 {
		return Config{}, errors.New(strings.Join(problems, "; "))
	}
	return cfg, nil
}
