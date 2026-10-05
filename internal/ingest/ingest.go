// Package ingest turns the events taken from the bus into rows.
package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/likho-ai/likho-analytics/internal/events"
	"github.com/likho-ai/likho-analytics/internal/metrics"
	"github.com/likho-ai/likho-analytics/internal/store"
)

const (
	recordingUpdated = "likho.recording.updated.v1"
	recordingDeleted = "likho.recording.deleted.v1"
)

// Ingester stores every event, and keeps the recordings' facts beside them.
type Ingester struct {
	store   *store.Store
	log     *slog.Logger
	metrics *metrics.Metrics
}

// New makes an ingester.
func New(s *store.Store, log *slog.Logger) *Ingester {
	return &Ingester{store: s, log: log}
}

// WithMetrics has the ingester count what it does.
func (i *Ingester) WithMetrics(m *metrics.Metrics) *Ingester {
	i.metrics = m
	return i
}

// Handle stores one event. An error has the bus deliver it again.
func (i *Ingester) Handle(ctx context.Context, subject string, event events.Event) error {
	var data map[string]any
	_ = json.Unmarshal(event.Data, &data)
	at, err := time.Parse(time.RFC3339Nano, event.Time)
	if err != nil {
		at = time.Now()
	}
	row := store.Event{
		ID: event.ID, Type: event.Type, Source: event.Source, Subject: subject, Time: at,
		WorkspaceID: text(data["workspace_id"]), RecordingID: text(data["recording_id"]), Data: string(event.Data),
	}
	if row.RecordingID == "" && event.Subject != "" && len(event.Subject) > 4 && event.Subject[:4] == "rec_" {
		row.RecordingID = event.Subject
	}
	if err := i.store.InsertEvent(ctx, row); err != nil {
		i.count(subject, "failed")
		return err
	}
	switch event.Type {
	case recordingUpdated:
		if err := i.store.UpsertRecording(ctx, recordingOf(data, at, false)); err != nil {
			i.count(subject, "failed")
			return err
		}
	case recordingDeleted:
		if err := i.store.UpsertRecording(ctx, recordingOf(data, at, true)); err != nil {
			i.count(subject, "failed")
			return err
		}
	}
	if i.metrics != nil {
		i.metrics.EventsStored.Add(ctx, 1, metrics.Type(event.Type))
	}
	i.count(subject, "ok")
	return nil
}

func (i *Ingester) count(subject, outcome string) {
	if i.metrics != nil {
		i.metrics.EventsHandled.Add(context.Background(), 1, metrics.Subject(subject), metrics.Outcome(outcome))
	}
}

// recordingOf reads a recording's facts from likho.recording.updated (or deleted).
func recordingOf(data map[string]any, at time.Time, deleted bool) store.Recording {
	r := store.Recording{
		RecordingID: text(data["recording_id"]),
		WorkspaceID: text(data["workspace_id"]),
		Source:      text(data["source"]),
		Name:        text(data["name"]),
		CallTime:    at,
		UpdatedAt:   at,
		Deleted:     deleted,
	}
	if callTime, err := time.Parse(time.RFC3339Nano, text(data["call_time"])); err == nil {
		r.CallTime = callTime
	}
	if attributes, ok := data["attributes"].(map[string]any); ok {
		r.Campaign = text(attributes["campaign"])
		r.Agent = text(attributes["agent"])
		r.Disposition = text(attributes["disposition"])
	}
	return r
}

func text(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}
