// Command likho-analytics runs the analytics service.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/likho-ai/likho-analytics/internal/app"
	"github.com/likho-ai/likho-analytics/internal/config"
)

func main() {
	os.Exit(run())
}

func run() int {
	if _, err := config.LoadEnvFiles(); err != nil {
		slog.New(slog.NewJSONHandler(os.Stdout, nil)).Error("configuration", "error", err)
		return 2
	}
	cfg, err := config.Load()
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stdout, nil)).Error("configuration", "error", err)
		return 2
	}
	log := newLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Long enough to wait for NATS (NATS_CONNECT_TIMEOUT_SECONDS) and still check the rest.
	startCtx, cancel := context.WithTimeout(ctx, cfg.NATSConnectTimeout+30*time.Second)
	service, err := app.New(startCtx, cfg, log)
	cancel()
	if err != nil {
		log.Error("could not start", "error", err)
		return 1
	}
	if err := service.Run(ctx); err != nil {
		log.Error("stopped with an error", "error", err)
		return 1
	}
	return 0
}

// One JSON object per log line, with the same field names as the other Likho services.
func newLogger(level string) *slog.Logger {
	var minimum slog.Level
	switch strings.ToUpper(level) {
	case "DEBUG":
		minimum = slog.LevelDebug
	case "WARN", "WARNING":
		minimum = slog.LevelWarn
	case "ERROR":
		minimum = slog.LevelError
	default:
		minimum = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: minimum,
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			switch attr.Key {
			case slog.MessageKey:
				attr.Key = "message"
			case slog.LevelKey:
				attr.Value = slog.StringValue(strings.ToLower(attr.Value.String()))
			}
			return attr
		},
	}))
}
