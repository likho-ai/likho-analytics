// Package app puts the service together and runs it.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"connectrpc.com/grpchealth"
	"github.com/likho-ai/likho-contracts/packages/go/gen/likho/analytics/v1/analyticsv1connect"

	"github.com/likho-ai/likho-analytics/internal/config"
	"github.com/likho-ai/likho-analytics/internal/events"
	"github.com/likho-ai/likho-analytics/internal/ingest"
	"github.com/likho-ai/likho-analytics/internal/metrics"
	"github.com/likho-ai/likho-analytics/internal/rpc"
	"github.com/likho-ai/likho-analytics/internal/store"
)

// Version of the service, shown in the start-up log line.
const Version = "0.1.0"

// App is the running service.
type App struct {
	cfg      config.Config
	log      *slog.Logger
	store    *store.Store
	bus      *events.Bus
	ingester *ingest.Ingester

	httpListener net.Listener
	grpcListener net.Listener
	httpServer   *http.Server
	grpcServer   *http.Server
	health       *grpchealth.StaticChecker
}

// New connects to everything the service needs and opens its ports. Nothing is served
// until Run is called.
func New(ctx context.Context, cfg config.Config, log *slog.Logger) (*App, error) {
	st, err := store.Open(ctx, cfg.ClickHouseURL, cfg.Location, log)
	if err != nil {
		return nil, err
	}
	if cfg.MigrateOnStart {
		if err := st.Migrate(ctx, cfg.RetentionDays); err != nil {
			_ = st.Close()
			return nil, err
		}
	}
	bus, err := events.Connect(ctx, cfg.NATSURL)
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	fail := func(err error) (*App, error) {
		bus.Close()
		_ = st.Close()
		return nil, err
	}
	httpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.HTTPPort))
	if err != nil {
		return fail(err)
	}
	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		_ = httpListener.Close()
		return fail(err)
	}
	meters, err := metrics.New(ctx, "likho-analytics", Version, cfg.OTLPEndpoint)
	if err != nil {
		_ = httpListener.Close()
		_ = grpcListener.Close()
		return fail(err)
	}
	ingester := ingest.New(st, log).WithMetrics(meters)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", meters.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if !bus.Connected() || st.Ping(ctx) != nil {
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})

	rpcMux := http.NewServeMux()
	rpcMux.Handle(analyticsv1connect.NewAnalyticsServiceHandler(rpc.New(st, log)))
	health := grpchealth.NewStaticChecker(analyticsv1connect.AnalyticsServiceName)
	rpcMux.Handle(grpchealth.NewHandler(health))

	// gRPC clients speak HTTP/2 without TLS inside the cluster.
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	return &App{
		cfg: cfg, log: log, store: st, bus: bus, ingester: ingester,
		httpListener: httpListener,
		grpcListener: grpcListener,
		httpServer:   &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second},
		grpcServer:   &http.Server{Handler: meters.Timed(rpcMux), Protocols: protocols, ReadHeaderTimeout: 10 * time.Second},
		health:       health,
	}, nil
}

// HTTPAddr is the address the HTTP side listens on ("127.0.0.1:4070").
func (a *App) HTTPAddr() string { return localAddr(a.httpListener) }

// GRPCAddr is the address the gRPC side listens on.
func (a *App) GRPCAddr() string { return localAddr(a.grpcListener) }

// Bus is the event bus connection (tests publish through it).
func (a *App) Bus() *events.Bus { return a.bus }

// Store is the ClickHouse connection (tests drop their database through it).
func (a *App) Store() *store.Store { return a.store }

func localAddr(listener net.Listener) string {
	return fmt.Sprintf("127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port)
}

// Run serves until ctx is cancelled, then stops cleanly.
func (a *App) Run(ctx context.Context) error {
	failed := make(chan error, 2)
	serve := func(server *http.Server, listener net.Listener) {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			failed <- err
		}
	}
	go serve(a.httpServer, a.httpListener)
	go serve(a.grpcServer, a.grpcListener)

	if a.cfg.ConsumersEnabled {
		fromNow := a.cfg.ConsumerStart == "new"
		group := a.cfg.ConsumerGroup
		if err := a.bus.Take(ctx, events.StreamEvents, events.AllEvents, group+"-events", fromNow, a.ingester.Handle, a.log); err != nil {
			return err
		}
		if err := a.bus.Take(ctx, events.StreamKeep, events.Corrections, group+"-corrections", fromNow, a.ingester.Handle, a.log); err != nil {
			return err
		}
	}
	a.log.Info(fmt.Sprintf("likho-analytics %s: HTTP on %s, gRPC on %s, days begin in %s, consumers %s",
		Version, a.httpListener.Addr(), a.grpcListener.Addr(), a.cfg.Location, onOff(a.cfg.ConsumersEnabled)))

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-failed:
	}

	a.log.Info("stopping")
	a.health.SetStatus(analyticsv1connect.AnalyticsServiceName, grpchealth.StatusNotServing)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = a.httpServer.Shutdown(shutdownCtx)
	_ = a.grpcServer.Shutdown(shutdownCtx)
	a.bus.Close()
	_ = a.store.Close()
	return runErr
}

func onOff(on bool) string {
	if on {
		return "on"
	}
	return "off"
}
