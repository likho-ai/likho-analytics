// Package store keeps every event in ClickHouse and answers the questions about them.
//
// Two tables: events (every CloudEvent, raw) and recordings (one row per change of a recording's
// facts, from likho.recording.updated). The numbers are computed at query time from both - one
// row per call, then grouped - which is fast enough for years of calls at this size; the views
// calls_daily, language_mix, agent_daily and insights_daily give people and dashboards the same
// numbers by day without a query of their own, and are what to materialise when volume demands.
package store

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Store is the connection to ClickHouse.
type Store struct {
	conn     driver.Conn
	options  *clickhouse.Options
	database string
	location *time.Location
	log      *slog.Logger
}

// Open connects to http[s]://user:password@host:8123/database and checks the connection.
// The database is created when it does not exist and the login may.
func Open(ctx context.Context, rawURL string, location *time.Location, log *slog.Logger) (*Store, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" {
		return nil, fmt.Errorf("CLICKHOUSE_URL %q is not a URL", rawURL)
	}
	database := strings.Trim(parsed.Path, "/")
	if database == "" {
		database = "likho_analytics"
	}
	password, _ := parsed.User.Password()
	options := &clickhouse.Options{
		Addr:        []string{parsed.Host},
		Protocol:    clickhouse.HTTP,
		Auth:        clickhouse.Auth{Database: database, Username: parsed.User.Username(), Password: password},
		DialTimeout: 5 * time.Second,
		ReadTimeout: 60 * time.Second,
		// Inserts are small and many: the server gathers them into parts (and answers once they are safe).
		Settings: clickhouse.Settings{"async_insert": 1, "wait_for_async_insert": 1},
	}
	if parsed.Scheme == "https" {
		options.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	s := &Store{options: options, database: database, location: location, log: log}
	s.ensureDatabase(ctx)
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse at %s: %w", parsed.Host, err)
	}
	s.conn = conn
	return s, nil
}

// ensureDatabase creates the database when the login may; otherwise the ping below says what is wrong.
func (s *Store) ensureDatabase(ctx context.Context) {
	admin := *s.options
	admin.Auth.Database = ""
	conn, err := clickhouse.Open(&admin)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdentifier(s.database)); err != nil {
		s.log.Debug("could not make sure the database exists", "database", s.database, "error", err)
	}
}

// Ping reports whether ClickHouse answers.
func (s *Store) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }

// Close closes the connection.
func (s *Store) Close() error { return s.conn.Close() }

// Drop removes the database (tests, which use a database of their own).
func (s *Store) Drop(ctx context.Context) error {
	return s.conn.Exec(ctx, "DROP DATABASE IF EXISTS "+quoteIdentifier(s.database))
}

// Migrate creates or updates the tables and the views. Safe to run at every start.
func (s *Store) Migrate(ctx context.Context, retentionDays int) error {
	zone := quoteString(s.location.String())
	statements := []string{
		`CREATE TABLE IF NOT EXISTS events (
			id String,
			type LowCardinality(String),
			source LowCardinality(String),
			subject String,
			time DateTime64(3, 'UTC'),
			workspace_id String,
			recording_id String,
			data String CODEC(ZSTD(3))
		) ENGINE = ReplacingMergeTree
		PARTITION BY toYYYYMM(time)
		ORDER BY (recording_id, type, time, id)
		TTL toDateTime(time) + INTERVAL ` + fmt.Sprint(retentionDays) + ` DAY`,
		`CREATE TABLE IF NOT EXISTS recordings (
			recording_id String,
			workspace_id String,
			source LowCardinality(String),
			name String,
			call_time DateTime64(3, 'UTC'),
			campaign String,
			agent String,
			disposition String,
			updated_at DateTime64(3, 'UTC'),
			deleted UInt8
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (workspace_id, recording_id)`,
		// One row per call with what became of it: the shape every question is asked of.
		`CREATE OR REPLACE VIEW call_facts AS
		WITH calls AS (` + callsSQL(false) + `),
		outcomes AS (` + outcomesSQL(false) + `)
		SELECT ` + factColumns + ` FROM calls c LEFT JOIN outcomes o USING (recording_id)`,
		// In a SELECT an alias shadows the column of the same name, so the sums carry names of their own.
		`CREATE OR REPLACE VIEW calls_daily AS
		SELECT workspace_id, toDate(call_time, ` + zone + `) AS day,
			count() AS calls, countIf(transcribed) AS transcribed_calls, countIf(failed) AS failed_calls,
			sumIf(audio_seconds, transcribed) / 60 AS minutes,
			sumIf(elapsed_seconds, transcribed) / nullIf(sumIf(audio_seconds, transcribed), 0) AS realtime_factor
		FROM call_facts GROUP BY workspace_id, day`,
		`CREATE OR REPLACE VIEW language_mix AS
		SELECT workspace_id, toDate(call_time, ` + zone + `) AS day, language, count() AS calls
		FROM call_facts WHERE transcribed GROUP BY workspace_id, day, language`,
		`CREATE OR REPLACE VIEW agent_daily AS
		SELECT workspace_id, toDate(call_time, ` + zone + `) AS day, agent,
			count() AS calls, countIf(transcribed) AS transcribed_calls, sumIf(audio_seconds, transcribed) / 60 AS minutes,
			countIf(analysed) AS analysed_calls, avgIf(score, analysed AND score IS NOT NULL) AS mean_score,
			countIf(analysed AND sentiment = 'negative') AS negative_calls
		FROM call_facts GROUP BY workspace_id, day, agent`,
		`CREATE OR REPLACE VIEW insights_daily AS
		SELECT workspace_id, toDate(call_time, ` + zone + `) AS day, sentiment, count() AS calls,
			avgIf(score, score IS NOT NULL) AS mean_score
		FROM call_facts WHERE analysed GROUP BY workspace_id, day, sentiment`,
	}
	for _, statement := range statements {
		if err := s.conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("clickhouse: %w\n%s", err, statement)
		}
	}
	return nil
}

// Event is one CloudEvent as stored.
type Event struct {
	ID          string
	Type        string
	Source      string
	Subject     string
	Time        time.Time
	WorkspaceID string
	RecordingID string
	Data        string
}

// InsertEvent stores one event. Storing the same id twice keeps one copy.
func (s *Store) InsertEvent(ctx context.Context, e Event) error {
	return s.conn.Exec(ctx,
		`INSERT INTO events (id, type, source, subject, time, workspace_id, recording_id, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		e.ID, e.Type, e.Source, e.Subject, e.Time.UTC(), e.WorkspaceID, e.RecordingID, e.Data)
}

// Recording is one change of a recording's facts.
type Recording struct {
	RecordingID string
	WorkspaceID string
	Source      string
	Name        string
	CallTime    time.Time
	Campaign    string
	Agent       string
	Disposition string
	UpdatedAt   time.Time
	Deleted     bool
}

// UpsertRecording records a recording's facts as of a moment; the latest row wins.
func (s *Store) UpsertRecording(ctx context.Context, r Recording) error {
	deleted := uint8(0)
	if r.Deleted {
		deleted = 1
	}
	return s.conn.Exec(ctx,
		`INSERT INTO recordings (recording_id, workspace_id, source, name, call_time, campaign, agent, disposition, updated_at, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.RecordingID, r.WorkspaceID, r.Source, r.Name, r.CallTime.UTC(), r.Campaign, r.Agent, r.Disposition, r.UpdatedAt.UTC(), deleted)
}

// Window is a span of call time: since included, until not.
type Window struct {
	Since time.Time
	Until time.Time
}

// Facts narrows to calls with these facts; empty = any.
type Facts struct {
	Campaign    string
	Agent       string
	Disposition string
	Source      string
	Language    string
}

// Count is a key with how many calls have it.
type Count struct {
	Key   string
	Count uint64
}

// Overview is what happened in a window.
type Overview struct {
	Calls          uint64
	Transcribed    uint64
	Failed         uint64
	Minutes        float64
	RealtimeFactor float64
	Analysed       uint64
	Score          float64
	Sentiments     []Count
	Languages      []Count
}

// Point is one bucket of a timeseries.
type Point struct {
	At    time.Time
	Value float64
}

// Row is one line of a breakdown.
type Row struct {
	Key         string
	Calls       uint64
	Transcribed uint64
	Minutes     float64
	Analysed    uint64
	Score       float64
	Negative    uint64
}

// Metrics a timeseries can show.
const (
	MetricCalls          = "calls"
	MetricTranscribed    = "transcribed"
	MetricMinutes        = "minutes"
	MetricRealtimeFactor = "realtime_factor"
	MetricAnalysed       = "analysed"
	MetricScore          = "score"
	MetricNegative       = "negative"
)

var metricSQL = map[string]string{
	MetricCalls:          "toFloat64(count())",
	MetricTranscribed:    "toFloat64(countIf(transcribed))",
	MetricMinutes:        "sumIf(audio_seconds, transcribed) / 60",
	MetricRealtimeFactor: "ifNull(sumIf(elapsed_seconds, transcribed) / nullIf(sumIf(audio_seconds, transcribed), 0), 0)",
	MetricAnalysed:       "toFloat64(countIf(analysed))",
	MetricScore:          "if(countIf(analysed AND score IS NOT NULL) = 0, 0, avgIf(score, analysed AND score IS NOT NULL))",
	MetricNegative:       "toFloat64(countIf(analysed AND sentiment = 'negative'))",
}

// Dimensions a breakdown can be by.
var dimensionSQL = map[string]string{
	"agent":       "if(agent = '', '(none)', agent)",
	"campaign":    "if(campaign = '', '(none)', campaign)",
	"disposition": "if(disposition = '', '(none)', disposition)",
	"language":    "if(language = '', '(none)', language)",
	"sentiment":   "if(sentiment = '', '(none)', sentiment)",
	"source":      "if(source = '', '(none)', source)",
}

// Buckets a timeseries can be in.
const (
	BucketDay  = "day"
	BucketHour = "hour"
)

// ErrBadRequest means the question itself was wrong (a dimension or metric that does not exist).
var ErrBadRequest = errors.New("bad request")

const factColumns = `c.recording_id AS recording_id, c.workspace_id AS workspace_id, c.source AS source,
	c.campaign AS campaign, c.agent AS agent, c.disposition AS disposition, c.call_time AS call_time,
	o.transcribed AS transcribed, o.audio_seconds AS audio_seconds, o.elapsed_seconds AS elapsed_seconds,
	o.language AS language, (o.ever_failed AND NOT o.transcribed) AS failed,
	o.analysed AS analysed, o.score AS score, o.sentiment AS sentiment`

// callsSQL is the latest facts of every recording that is not deleted; windowed when asked.
// The workspace is filtered on the raw rows (a subquery, so the alias cannot shadow the column);
// the window and the facts are checked on the latest values, in HAVING.
func callsSQL(windowed bool) string {
	source := "recordings"
	having := "HAVING deleted = 0"
	if windowed {
		source = "(SELECT * FROM recordings WHERE workspace_id = {ws:String})"
		having += " AND call_time >= {since:DateTime64(3, 'UTC')} AND call_time < {until:DateTime64(3, 'UTC')}"
	}
	return `SELECT recording_id,
		argMax(workspace_id, updated_at) AS workspace_id,
		argMax(source, updated_at) AS source,
		argMax(campaign, updated_at) AS campaign,
		argMax(agent, updated_at) AS agent,
		argMax(disposition, updated_at) AS disposition,
		argMax(call_time, updated_at) AS call_time,
		argMax(deleted, updated_at) AS deleted
	FROM ` + source + `
	GROUP BY recording_id ` + having
}

// outcomesSQL is what became of each recording, from its events.
func outcomesSQL(windowed bool) string {
	where := ""
	if windowed {
		where = "WHERE recording_id IN (SELECT recording_id FROM calls) AND time >= {since:DateTime64(3, 'UTC')} - INTERVAL 1 DAY"
	}
	return `SELECT recording_id,
		uniqExactIf(id, type = 'likho.transcription.completed.v1') > 0 AS transcribed,
		argMaxIf(JSONExtractFloat(data, 'stats', 'audio_seconds'), time, type = 'likho.transcription.completed.v1') AS audio_seconds,
		argMaxIf(JSONExtractFloat(data, 'stats', 'elapsed_seconds'), time, type = 'likho.transcription.completed.v1') AS elapsed_seconds,
		argMaxIf(JSONExtractString(data, 'language', 'detected'), time, type = 'likho.transcription.completed.v1') AS language,
		uniqExactIf(id, type = 'likho.transcription.failed.v1') > 0 AS ever_failed,
		uniqExactIf(id, type = 'likho.insights.completed.v1') > 0 AS analysed,
		argMaxIf(JSONExtractFloat(data, 'score_total') / nullIf(JSONExtractFloat(data, 'score_max'), 0), time, type = 'likho.insights.completed.v1') AS score,
		argMaxIf(JSONExtractString(data, 'sentiment'), time, type = 'likho.insights.completed.v1') AS sentiment
	FROM events ` + where + `
	GROUP BY recording_id`
}

// factsQuery is the common head: one row per call in the window with the facts asked for.
func factsQuery(ws string, w Window, f Facts) (string, []any) {
	args := []any{
		clickhouse.Named("ws", ws),
		clickhouse.Named("since", w.Since.UTC().Format("2006-01-02 15:04:05.000")),
		clickhouse.Named("until", w.Until.UTC().Format("2006-01-02 15:04:05.000")),
	}
	callFilters := ""
	for name, value := range map[string]string{"campaign": f.Campaign, "agent": f.Agent, "disposition": f.Disposition, "source": f.Source} {
		if value != "" {
			callFilters += fmt.Sprintf(" AND %s = {%s:String}", name, name)
			args = append(args, clickhouse.Named(name, value))
		}
	}
	factFilter := ""
	if f.Language != "" {
		factFilter = " WHERE language = {language:String}"
		args = append(args, clickhouse.Named("language", f.Language))
	}
	query := `WITH calls AS (` + callsSQL(true) + callFilters + `),
	outcomes AS (` + outcomesSQL(true) + `),
	facts AS (SELECT ` + factColumns + ` FROM calls c LEFT JOIN outcomes o USING (recording_id)` + factFilter + `)
	`
	return query, args
}

// Overview answers what happened in the window.
func (s *Store) Overview(ctx context.Context, ws string, w Window, f Facts) (Overview, error) {
	head, args := factsQuery(ws, w, f)
	var o Overview
	row := s.conn.QueryRow(ctx, head+`SELECT count(), countIf(transcribed), countIf(failed),
		sumIf(audio_seconds, transcribed) / 60,
		ifNull(sumIf(elapsed_seconds, transcribed) / nullIf(sumIf(audio_seconds, transcribed), 0), 0),
		countIf(analysed),
		if(countIf(analysed AND score IS NOT NULL) = 0, 0, avgIf(score, analysed AND score IS NOT NULL))
	FROM facts`, args...)
	if err := row.Scan(&o.Calls, &o.Transcribed, &o.Failed, &o.Minutes, &o.RealtimeFactor, &o.Analysed, &o.Score); err != nil {
		return Overview{}, fmt.Errorf("clickhouse: overview: %w", err)
	}
	var err error
	if o.Sentiments, err = s.counts(ctx, head+`SELECT sentiment, count() AS n FROM facts WHERE analysed GROUP BY sentiment ORDER BY n DESC, sentiment`, args); err != nil {
		return Overview{}, err
	}
	if o.Languages, err = s.counts(ctx, head+`SELECT language, count() AS n FROM facts WHERE transcribed GROUP BY language ORDER BY n DESC, language`, args); err != nil {
		return Overview{}, err
	}
	return o, nil
}

func (s *Store) counts(ctx context.Context, query string, args []any) ([]Count, error) {
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Count
	for rows.Next() {
		var c Count
		if err := rows.Scan(&c.Key, &c.Count); err != nil {
			return nil, fmt.Errorf("clickhouse: %w", err)
		}
		if c.Key == "" {
			c.Key = "(none)"
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Timeseries answers one metric per bucket across the window, with empty buckets at 0.
func (s *Store) Timeseries(ctx context.Context, ws string, w Window, f Facts, metric, bucket string) ([]Point, error) {
	expression, ok := metricSQL[metric]
	if !ok {
		return nil, fmt.Errorf("%w: metric %q", ErrBadRequest, metric)
	}
	var interval string
	var step func(time.Time) time.Time
	var start time.Time
	switch bucket {
	case BucketDay:
		interval = "INTERVAL 1 DAY"
		local := w.Since.In(s.location)
		start = time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, s.location)
		step = func(t time.Time) time.Time { return t.AddDate(0, 0, 1) }
	case BucketHour:
		interval = "INTERVAL 1 HOUR"
		start = w.Since.In(s.location).Truncate(time.Hour)
		step = func(t time.Time) time.Time { return t.Add(time.Hour) }
	default:
		return nil, fmt.Errorf("%w: bucket %q", ErrBadRequest, bucket)
	}
	head, args := factsQuery(ws, w, f)
	rows, err := s.conn.Query(ctx, head+`SELECT toStartOfInterval(call_time, `+interval+`, `+quoteString(s.location.String())+`) AS at, `+expression+`
		FROM facts GROUP BY at ORDER BY at`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: timeseries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	values := map[int64]float64{}
	for rows.Next() {
		var at time.Time
		var value float64
		if err := rows.Scan(&at, &value); err != nil {
			return nil, fmt.Errorf("clickhouse: timeseries: %w", err)
		}
		values[at.Unix()] = value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: timeseries: %w", err)
	}
	var points []Point
	for at := start; at.Before(w.Until) && len(points) < 10_000; at = step(at) {
		points = append(points, Point{At: at, Value: values[at.Unix()]})
	}
	return points, nil
}

// Breakdown answers the window's calls split by one dimension, largest first.
func (s *Store) Breakdown(ctx context.Context, ws string, w Window, f Facts, by string, limit int) ([]Row, error) {
	expression, ok := dimensionSQL[by]
	if !ok {
		return nil, fmt.Errorf("%w: dimension %q", ErrBadRequest, by)
	}
	if limit <= 0 {
		limit = 50
	}
	limit = min(limit, 500)
	head, args := factsQuery(ws, w, f)
	args = append(args, clickhouse.Named("limit", fmt.Sprint(limit)))
	rows, err := s.conn.Query(ctx, head+`SELECT `+expression+` AS key, count() AS calls, countIf(transcribed),
		sumIf(audio_seconds, transcribed) / 60, countIf(analysed),
		if(countIf(analysed AND score IS NOT NULL) = 0, 0, avgIf(score, analysed AND score IS NOT NULL)),
		countIf(analysed AND sentiment = 'negative')
		FROM facts GROUP BY key ORDER BY calls DESC, key LIMIT {limit:UInt32}`, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: breakdown: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.Key, &r.Calls, &r.Transcribed, &r.Minutes, &r.Analysed, &r.Score, &r.Negative); err != nil {
			return nil, fmt.Errorf("clickhouse: breakdown: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "") + "`"
}

func quoteString(value string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
}
