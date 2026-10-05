// Package testenv helps tests that need the likho-infra stack (ClickHouse and NATS).
//
// Start the stack first:  likho-infra> bash scripts/up.sh   (or .\stack.ps1 up)
// Without it these tests are skipped locally; with LIKHO_REQUIRE_STACK=1 (set in CI) they fail instead.
package testenv

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/likho-ai/likho-analytics/internal/config"
)

// Config returns the service's configuration for a test: the local stack, a database of its own
// (dropped by the test), free ports, a consumer group of its own that starts at the events
// published from now on, and days that begin in UTC.
func Config(t *testing.T) config.Config {
	t.Helper()
	t.Setenv("LIKHO_ENV", "test")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	requireStack(t, cfg)
	suffix := Unique()
	cfg.HTTPPort, cfg.GRPCPort = 0, 0
	parsed, err := url.Parse(cfg.ClickHouseURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/test_" + suffix
	cfg.ClickHouseURL = parsed.String()
	cfg.ConsumerGroup = "likho-analytics-test-" + suffix
	cfg.ConsumerStart = "new"
	cfg.Location = time.UTC
	return cfg
}

// Unique returns a short random id for names that must not collide between test runs.
func Unique() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func requireStack(t *testing.T, cfg config.Config) {
	t.Helper()
	var missing []string
	for name, address := range map[string]string{"ClickHouse": hostOf(cfg.ClickHouseURL), "NATS": hostOf(cfg.NATSURL)} {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			missing = append(missing, name)
			continue
		}
		_ = conn.Close()
	}
	if len(missing) == 0 {
		return
	}
	message := strings.Join(missing, ", ") + " not available; start the likho-infra stack"
	if os.Getenv("LIKHO_REQUIRE_STACK") == "1" {
		t.Fatal(message)
	}
	t.Skip(message)
}

func hostOf(address string) string {
	parsed, err := url.Parse(address)
	if err != nil {
		return address
	}
	return parsed.Host
}
