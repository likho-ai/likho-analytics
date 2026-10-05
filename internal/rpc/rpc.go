// Package rpc serves likho.analytics.v1.AnalyticsService.
package rpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	analyticsv1 "github.com/likho-ai/likho-contracts/packages/go/gen/likho/analytics/v1"
	"github.com/likho-ai/likho-contracts/packages/go/gen/likho/analytics/v1/analyticsv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/likho-ai/likho-analytics/internal/store"
)

// The longest window a question may span.
const maxWindow = 400 * 24 * time.Hour

// Server answers the questions.
type Server struct {
	store *store.Store
	log   *slog.Logger
}

var _ analyticsv1connect.AnalyticsServiceHandler = (*Server)(nil)

// New makes the server.
func New(s *store.Store, log *slog.Logger) *Server {
	return &Server{store: s, log: log}
}

// GetOverview answers the window's totals.
func (s *Server) GetOverview(ctx context.Context, req *connect.Request[analyticsv1.GetOverviewRequest]) (*connect.Response[analyticsv1.GetOverviewResponse], error) {
	ws, window, err := check(req.Msg.GetWorkspaceId(), req.Msg.GetWindow())
	if err != nil {
		return nil, err
	}
	o, err := s.store.Overview(ctx, ws, window, facts(req.Msg.GetFacts()))
	if err != nil {
		return nil, failed(err)
	}
	return connect.NewResponse(&analyticsv1.GetOverviewResponse{Overview: &analyticsv1.Overview{
		Calls: o.Calls, Transcribed: o.Transcribed, Failed: o.Failed, Minutes: o.Minutes,
		RealtimeFactor: o.RealtimeFactor, Analysed: o.Analysed, Score: o.Score,
		Sentiments: counts(o.Sentiments), Languages: counts(o.Languages),
	}}), nil
}

// GetTimeseries answers one metric per bucket.
func (s *Server) GetTimeseries(ctx context.Context, req *connect.Request[analyticsv1.GetTimeseriesRequest]) (*connect.Response[analyticsv1.GetTimeseriesResponse], error) {
	ws, window, err := check(req.Msg.GetWorkspaceId(), req.Msg.GetWindow())
	if err != nil {
		return nil, err
	}
	metric, ok := map[analyticsv1.Metric]string{
		analyticsv1.Metric_METRIC_CALLS:           store.MetricCalls,
		analyticsv1.Metric_METRIC_TRANSCRIBED:     store.MetricTranscribed,
		analyticsv1.Metric_METRIC_MINUTES:         store.MetricMinutes,
		analyticsv1.Metric_METRIC_REALTIME_FACTOR: store.MetricRealtimeFactor,
		analyticsv1.Metric_METRIC_ANALYSED:        store.MetricAnalysed,
		analyticsv1.Metric_METRIC_SCORE:           store.MetricScore,
		analyticsv1.Metric_METRIC_NEGATIVE:        store.MetricNegative,
	}[req.Msg.GetMetric()]
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("a metric is required"))
	}
	bucket := store.BucketDay
	switch req.Msg.GetBucket() {
	case analyticsv1.Bucket_BUCKET_HOUR:
		bucket = store.BucketHour
		if window.Until.Sub(window.Since) > 31*24*time.Hour {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("hours are for windows of a month at most"))
		}
	case analyticsv1.Bucket_BUCKET_DAY, analyticsv1.Bucket_BUCKET_UNSPECIFIED:
	}
	points, err := s.store.Timeseries(ctx, ws, window, facts(req.Msg.GetFacts()), metric, bucket)
	if err != nil {
		return nil, failed(err)
	}
	out := make([]*analyticsv1.Point, 0, len(points))
	for _, p := range points {
		out = append(out, &analyticsv1.Point{At: timestamppb.New(p.At), Value: p.Value})
	}
	return connect.NewResponse(&analyticsv1.GetTimeseriesResponse{Points: out}), nil
}

// GetBreakdown answers the window's calls by one dimension.
func (s *Server) GetBreakdown(ctx context.Context, req *connect.Request[analyticsv1.GetBreakdownRequest]) (*connect.Response[analyticsv1.GetBreakdownResponse], error) {
	ws, window, err := check(req.Msg.GetWorkspaceId(), req.Msg.GetWindow())
	if err != nil {
		return nil, err
	}
	by, ok := map[analyticsv1.Dimension]string{
		analyticsv1.Dimension_DIMENSION_AGENT:       "agent",
		analyticsv1.Dimension_DIMENSION_CAMPAIGN:    "campaign",
		analyticsv1.Dimension_DIMENSION_DISPOSITION: "disposition",
		analyticsv1.Dimension_DIMENSION_LANGUAGE:    "language",
		analyticsv1.Dimension_DIMENSION_SENTIMENT:   "sentiment",
		analyticsv1.Dimension_DIMENSION_SOURCE:      "source",
	}[req.Msg.GetBy()]
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("a dimension is required"))
	}
	rows, err := s.store.Breakdown(ctx, ws, window, facts(req.Msg.GetFacts()), by, int(req.Msg.GetLimit()))
	if err != nil {
		return nil, failed(err)
	}
	out := make([]*analyticsv1.Row, 0, len(rows))
	for _, r := range rows {
		out = append(out, &analyticsv1.Row{
			Key: r.Key, Calls: r.Calls, Transcribed: r.Transcribed, Minutes: r.Minutes,
			Analysed: r.Analysed, Score: r.Score, Negative: r.Negative,
		})
	}
	return connect.NewResponse(&analyticsv1.GetBreakdownResponse{Rows: out}), nil
}

func check(ws string, w *analyticsv1.Window) (string, store.Window, error) {
	if ws == "" {
		return "", store.Window{}, connect.NewError(connect.CodeInvalidArgument, errors.New("workspace_id is required"))
	}
	if w == nil || w.GetSince() == nil || w.GetUntil() == nil {
		return "", store.Window{}, connect.NewError(connect.CodeInvalidArgument, errors.New("a window with since and until is required"))
	}
	window := store.Window{Since: w.GetSince().AsTime(), Until: w.GetUntil().AsTime()}
	if !window.Until.After(window.Since) {
		return "", store.Window{}, connect.NewError(connect.CodeInvalidArgument, errors.New("until must come after since"))
	}
	if window.Until.Sub(window.Since) > maxWindow {
		return "", store.Window{}, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("a window spans at most %d days", int(maxWindow.Hours()/24)))
	}
	return ws, window, nil
}

func facts(f *analyticsv1.Facts) store.Facts {
	if f == nil {
		return store.Facts{}
	}
	return store.Facts{
		Campaign: f.GetCampaign(), Agent: f.GetAgent(), Disposition: f.GetDisposition(),
		Source: f.GetSource(), Language: f.GetLanguage(),
	}
}

func counts(in []store.Count) []*analyticsv1.Count {
	out := make([]*analyticsv1.Count, 0, len(in))
	for _, c := range in {
		out = append(out, &analyticsv1.Count{Key: c.Key, Count: c.Count})
	}
	return out
}

func failed(err error) error {
	if errors.Is(err, store.ErrBadRequest) {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewError(connect.CodeUnavailable, err)
}
