# likho-analytics

The analytics service of [Likho](https://github.com/likho-ai): the numbers behind the calls.
It keeps every event of the platform in ClickHouse and answers what happened in a window:

| Question | Call |
| --- | --- |
| How many calls came in, how many were transcribed or failed, how many minutes, how fast, how many the model analysed and how they scored, in which languages, in which mood? | `GetOverview` |
| The same, one number per day or hour, for a chart | `GetTimeseries` |
| The same, split by agent, campaign, disposition, language, sentiment or source | `GetBreakdown` |

Every question is asked for one workspace, a window of call time (the dialer's call time, else
when the recording was made) and, if wanted, the calls' facts (campaign, agent, disposition,
source, language). The interface is `likho.analytics.v1.AnalyticsService` in
[likho-contracts](https://github.com/likho-ai/likho-contracts).

## How it counts

- **`events`** holds every CloudEvent on the bus (`likho.>` on the LIKHO stream and the
  corrections on LIKHO_KEEP), raw, with the event's type, time, workspace and recording. Storing
  the same event twice keeps one copy (ReplacingMergeTree by id).
- **`recordings`** holds a recording's facts every time they change (`likho.recording.updated`:
  source, name, call time, campaign, agent, disposition) and whether it was deleted.
- A question joins the two into one row per call - transcribed or failed, minutes of audio and
  seconds of work, the language detected, analysed, the score and the sentiment - and groups.
  At this size (tens of thousands of calls a day) that takes milliseconds for a day and well
  under a second for a year.
- The views **`calls_daily`**, **`language_mix`**, **`agent_daily`** and **`insights_daily`**
  give the same numbers by day for people and dashboards that query ClickHouse directly; they
  are what to materialise when the volume demands it.

A day begins in `TIMEZONE` (the company's zone). A score is the share of the form's maximum
(0 to 1), averaged over the analysed calls.

## Run it

Needs the [likho-infra](https://github.com/likho-ai/likho-infra) stack (ClickHouse and NATS).

```bash
go run ./cmd/likho-analytics       # HTTP 4070 (/healthz, /readyz, /metrics), gRPC 5070
```

The tables and views are made at start (`MIGRATE_ON_START`). A new installation starts with
`CONSUMER_START=all` and takes every event the streams still hold (30 days), so the numbers
are there from the first minute.

With Docker, on the stack's network:

```bash
docker build -t likho-analytics .
docker run --rm --network likho -p 4070:4070 -p 5070:5070 \
  -e CLICKHOUSE_URL=http://likho_analytics:likho_analytics@clickhouse:8123/likho_analytics \
  -e NATS_URL=nats://nats:4222 likho-analytics
```

## Configuration

Settings come from environment variables and from `.env` files chosen by `LIKHO_ENV`
(`development` by default). The files are read in this order, each overriding the one before,
and a real environment variable wins over all of them:

```
.env   .env.local   .env.<LIKHO_ENV>   .env.<LIKHO_ENV>.local
```

`.env.development`, `.env.staging` and `.env.production` are committed and hold no secrets.
`.env.<env>.local` holds the secrets of that environment on your machine; git ignores it, and
`likho-infra/scripts/make-env-secrets.py` makes it. In Kubernetes the same values come from
ConfigMaps and Secrets.

| Variable | Default | Meaning |
| --- | --- | --- |
| `HTTP_PORT` | `4070` | `GET /healthz` (alive), `GET /readyz` (ClickHouse and the bus answer), `GET /metrics` |
| `GRPC_PORT` | `5070` | gRPC, including the standard health service |
| `CLICKHOUSE_URL` | local stack, database `likho_analytics` | `http[s]://user:password@host:8123/database`; the database is created when the login may |
| `MIGRATE_ON_START` | `true` | Create or update the tables and views at start |
| `RETENTION_DAYS` | `400` | How long events are kept |
| `TIMEZONE` | `TZ`, else `UTC` | The zone a day begins in |
| `NATS_URL` | `nats://localhost:4222` | Event bus |
| `NATS_CONNECT_TIMEOUT_SECONDS` | `120` | How long the start keeps trying to reach NATS before giving up |
| `CONSUMERS_ENABLED` | `true` | Take the events (false for an instance that only answers) |
| `CONSUMER_GROUP` | `likho-analytics` | Durable consumers `<group>-events` and `<group>-corrections`; instances with the same name share the work |
| `CONSUMER_START` | `all` | Where a group new to the bus starts: `all` takes every event the streams hold, `new` only those from now on |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty | Also push the metrics there (OTLP/HTTP) |
| `LOG_LEVEL` | `INFO` | Logs are JSON, one object per line |

Metrics at `GET /metrics`: calls by method and status and their duration, events stored by type,
events handled by subject and outcome, and `likho_analytics_up`.

## Work on it

```bash
go vet ./... && go test ./...      # the service test needs ClickHouse and NATS from likho-infra
```

The test runs the real service against ClickHouse (a database of its own, dropped afterwards)
and NATS (a consumer group of its own that starts at the events it publishes). Without the stack
it is skipped; with `LIKHO_REQUIRE_STACK=1` (set in CI) it fails instead.
