package app_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	analyticsv1 "github.com/likho-ai/likho-contracts/packages/go/gen/likho/analytics/v1"
	"github.com/likho-ai/likho-contracts/packages/go/gen/likho/analytics/v1/analyticsv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/likho-ai/likho-analytics/internal/app"
	"github.com/likho-ai/likho-analytics/internal/events"
	"github.com/likho-ai/likho-analytics/internal/testenv"
)

type running struct {
	app    *app.App
	client analyticsv1connect.AnalyticsServiceClient
	bus    *events.Bus
}

func start(t *testing.T) running {
	t.Helper()
	cfg := testenv.Config(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	service, err := app.New(ctx, cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(runCtx) }()
	t.Cleanup(func() {
		service.Bus().Forget(context.Background())
		_ = service.Store().Drop(context.Background())
		stop()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("the service did not stop in time")
		}
	})
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	httpClient := &http.Client{Transport: &http.Transport{Protocols: protocols}}
	client := analyticsv1connect.NewAnalyticsServiceClient(httpClient, "http://"+service.GRPCAddr(), connect.WithGRPC())
	// The consumers are made when Run starts; give them a moment before publishing.
	time.Sleep(500 * time.Millisecond)
	return running{app: service, client: client, bus: service.Bus()}
}

func window(since, until time.Time) *analyticsv1.Window {
	return &analyticsv1.Window{Since: timestamppb.New(since), Until: timestamppb.New(until)}
}

func TestTheNumbersBehindTheCalls(t *testing.T) {
	r := start(t)
	ctx := context.Background()
	ws := "wsp_" + strings.ToUpper(testenv.Unique())
	rec := func() string { return "rec_" + strings.ToUpper(testenv.Unique()) }
	a, b, c := rec(), rec(), rec()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	callTime := today.Add(10 * time.Hour)
	since, until := today.AddDate(0, 0, -1), today.AddDate(0, 0, 2)

	publish := func(subject, eventType string, recording string, data map[string]any) {
		t.Helper()
		id := "evt_" + testenv.Unique()
		data["recording_id"] = recording
		data["workspace_id"] = ws
		err := r.bus.Publish(ctx, subject, id, map[string]any{
			"specversion": "1.0", "id": id, "source": "test", "type": eventType,
			"time": time.Now().UTC().Format(time.RFC3339Nano), "subject": recording,
			"datacontenttype": "application/json", "data": data,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	updated := func(recording, agent, campaign string) {
		publish("likho.recording.updated", "likho.recording.updated.v1", recording, map[string]any{
			"source": "ameyo", "name": recording + ".mp3", "call_time": callTime.Format(time.RFC3339Nano),
			"attributes": map[string]any{"campaign": campaign, "agent": agent, "disposition": "sold"},
		})
	}
	completed := func(recording, language string, audio, elapsed float64) {
		publish("likho.transcription.completed", "likho.transcription.completed.v1", recording, map[string]any{
			"job_id": "job_1", "transcript_id": "trn_" + recording, "version": 1,
			"language": map[string]any{"detected": language, "probability": 0.9},
			"stats":    map[string]any{"audio_seconds": audio, "elapsed_seconds": elapsed, "segments": 10},
		})
	}

	// Three calls: two transcribed (one of them analysed), one that failed.
	updated(a, "asha", "sale")
	updated(b, "asha", "sale")
	updated(c, "ravi", "support")
	completed(a, "hi", 120, 60)
	completed(b, "ur", 60, 90)
	publish("likho.transcription.failed", "likho.transcription.failed.v1", c, map[string]any{"job_id": "job_3", "code": "stalled", "message": "x"})
	publish("likho.insights.completed", "likho.insights.completed.v1", a, map[string]any{
		"insights_id": "ins_1", "transcript_id": "trn_" + a, "model": "fake/one", "sentiment": "positive",
		"score_total": 16, "score_max": 20, "input_tokens": 1, "output_tokens": 1,
	})

	overview := waitForCalls(t, r.client, ws, window(since, until), 3)
	want := &analyticsv1.Overview{
		Calls: 3, Transcribed: 2, Failed: 1, Minutes: 3, RealtimeFactor: 150.0 / 180.0, Analysed: 1, Score: 0.8,
		Sentiments: []*analyticsv1.Count{{Key: "positive", Count: 1}},
		Languages:  []*analyticsv1.Count{{Key: "hi", Count: 1}, {Key: "ur", Count: 1}},
	}
	if overview.GetCalls() != want.Calls || overview.GetTranscribed() != want.Transcribed || overview.GetFailed() != want.Failed ||
		!close(overview.GetMinutes(), want.Minutes) || !close(overview.GetRealtimeFactor(), want.RealtimeFactor) ||
		overview.GetAnalysed() != want.Analysed || !close(overview.GetScore(), want.Score) {
		t.Fatalf("overview: got %v, want %v", overview, want)
	}
	if fmt.Sprint(overview.GetSentiments()) != fmt.Sprint(want.Sentiments) || fmt.Sprint(overview.GetLanguages()) != fmt.Sprint(want.Languages) {
		t.Fatalf("overview mixes: got %v / %v", overview.GetSentiments(), overview.GetLanguages())
	}

	// Narrowed by a fact: only the support call, which failed.
	narrowed, err := r.client.GetOverview(ctx, connect.NewRequest(&analyticsv1.GetOverviewRequest{
		WorkspaceId: ws, Window: window(since, until), Facts: &analyticsv1.Facts{Campaign: "support"},
	}))
	if err != nil || narrowed.Msg.GetOverview().GetCalls() != 1 || narrowed.Msg.GetOverview().GetFailed() != 1 {
		t.Fatalf("narrowed overview: %v %v", narrowed, err)
	}

	// One point per day across the window, the calls on their day.
	series, err := r.client.GetTimeseries(ctx, connect.NewRequest(&analyticsv1.GetTimeseriesRequest{
		WorkspaceId: ws, Window: window(since, until), Metric: analyticsv1.Metric_METRIC_CALLS, Bucket: analyticsv1.Bucket_BUCKET_DAY,
	}))
	if err != nil {
		t.Fatal(err)
	}
	var values []float64
	for _, p := range series.Msg.GetPoints() {
		values = append(values, p.GetValue())
	}
	if fmt.Sprint(values) != "[0 3 0]" || !series.Msg.GetPoints()[1].GetAt().AsTime().Equal(today) {
		t.Fatalf("timeseries: %v", series.Msg.GetPoints())
	}
	minutes, err := r.client.GetTimeseries(ctx, connect.NewRequest(&analyticsv1.GetTimeseriesRequest{
		WorkspaceId: ws, Window: window(since, until), Metric: analyticsv1.Metric_METRIC_MINUTES,
	}))
	if err != nil || !close(minutes.Msg.GetPoints()[1].GetValue(), 3) {
		t.Fatalf("minutes: %v %v", minutes, err)
	}

	// By agent: asha has the two transcribed calls, one analysed; ravi the failed one.
	breakdown, err := r.client.GetBreakdown(ctx, connect.NewRequest(&analyticsv1.GetBreakdownRequest{
		WorkspaceId: ws, Window: window(since, until), By: analyticsv1.Dimension_DIMENSION_AGENT,
	}))
	if err != nil {
		t.Fatal(err)
	}
	rows := breakdown.Msg.GetRows()
	if len(rows) != 2 || rows[0].GetKey() != "asha" || rows[0].GetCalls() != 2 || rows[0].GetTranscribed() != 2 ||
		!close(rows[0].GetMinutes(), 3) || rows[0].GetAnalysed() != 1 || !close(rows[0].GetScore(), 0.8) ||
		rows[1].GetKey() != "ravi" || rows[1].GetCalls() != 1 {
		t.Fatalf("breakdown by agent: %v", rows)
	}
	languages, err := r.client.GetBreakdown(ctx, connect.NewRequest(&analyticsv1.GetBreakdownRequest{
		WorkspaceId: ws, Window: window(since, until), By: analyticsv1.Dimension_DIMENSION_LANGUAGE,
	}))
	if err != nil || len(languages.Msg.GetRows()) != 3 { // hi, ur, and (none) for the failed call
		t.Fatalf("breakdown by language: %v %v", languages, err)
	}

	// A deleted recording leaves the numbers.
	publish("likho.recording.deleted", "likho.recording.deleted.v1", c, map[string]any{})
	after := waitForCalls(t, r.client, ws, window(since, until), 2)
	if after.GetFailed() != 0 {
		t.Fatalf("the deleted call still counts: %v", after)
	}

	// Another workspace sees nothing; a question without a workspace or a window is refused.
	other, err := r.client.GetOverview(ctx, connect.NewRequest(&analyticsv1.GetOverviewRequest{WorkspaceId: "wsp_other", Window: window(since, until)}))
	if err != nil || other.Msg.GetOverview().GetCalls() != 0 {
		t.Fatalf("another workspace: %v %v", other, err)
	}
	if _, err := r.client.GetOverview(ctx, connect.NewRequest(&analyticsv1.GetOverviewRequest{Window: window(since, until)})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("no workspace: %v", err)
	}
	if _, err := r.client.GetOverview(ctx, connect.NewRequest(&analyticsv1.GetOverviewRequest{WorkspaceId: ws})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("no window: %v", err)
	}
	if _, err := r.client.GetTimeseries(ctx, connect.NewRequest(&analyticsv1.GetTimeseriesRequest{WorkspaceId: ws, Window: window(since, until)})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("no metric: %v", err)
	}

	// Health and metrics.
	resp, err := http.Get("http://" + r.app.HTTPAddr() + "/readyz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz: %v %v", resp, err)
	}
	_ = resp.Body.Close()
	resp, err = http.Get("http://" + r.app.HTTPAddr() + "/metrics")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics: %v %v", resp, err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), "likho_analytics_requests_total") || !strings.Contains(string(body), "likho_analytics_events_stored_total") {
		t.Fatalf("metrics: %s", body)
	}
}

// waitForCalls polls the overview until it counts the expected calls.
func waitForCalls(t *testing.T, client analyticsv1connect.AnalyticsServiceClient, ws string, w *analyticsv1.Window, calls uint64) *analyticsv1.Overview {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		response, err := client.GetOverview(context.Background(), connect.NewRequest(&analyticsv1.GetOverviewRequest{WorkspaceId: ws, Window: w}))
		if err == nil && response.Msg.GetOverview().GetCalls() == calls {
			// The events of those calls may still be landing; one more moment, then read again.
			time.Sleep(700 * time.Millisecond)
			response, err = client.GetOverview(context.Background(), connect.NewRequest(&analyticsv1.GetOverviewRequest{WorkspaceId: ws, Window: w}))
			if err == nil {
				return response.Msg.GetOverview()
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				t.Fatalf("overview: %v", err)
			}
			t.Fatalf("expected %d calls, got %d", calls, response.Msg.GetOverview().GetCalls())
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func close(a, b float64) bool { return math.Abs(a-b) < 0.001 }
