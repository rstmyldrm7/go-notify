# go-notify

Event-driven notification system that accepts messages over a REST API and
delivers them through multiple channels (SMS, Email, Push) with priority
queues, per-channel rate limiting, scheduled delivery, retries, a dead-letter
queue and full observability — metrics, dashboards and distributed tracing.

```
Client ──REST──▶ API ──▶ PostgreSQL (source of truth)
                  │
                  └────▶ Kafka (channel × priority topics)
                            │
                            ▼
                     Worker pools ──▶ Provider (mock / webhook.site)
                            │
                  Scheduler & Reaper (scheduled dispatch + reconciliation)
```

---

## Quick start

One command — no external dependencies, no configuration:

```bash
docker compose up -d --build
```

This starts the full system: PostgreSQL, Kafka (with topics pre-created), the
three application services, a bundled mock provider, Prometheus, Grafana and
Tempo. Migrations are applied automatically on API startup.

Send your first notification:

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Hello!","priority":"high"}'
```

Within a second it travels API → Kafka → worker → provider and lands in
status `sent`:

```bash
curl http://localhost:8080/api/v1/notifications/<id-from-response>
```

### Where to look

| What                       | Where                            | Notes                                          |
|----------------------------|----------------------------------|------------------------------------------------|
| REST API                   | http://localhost:8080            | `GET /healthz` for liveness                    |
| **Swagger UI**             | http://localhost:8080/docs       | interactive API docs (spec: `/openapi.yaml`)   |
| **Grafana**                | http://localhost:3000            | no login; "go-notify" dashboard pre-loaded     |
| Traces (Tempo)             | Grafana → Explore → Tempo        | end-to-end request waterfalls                  |
| Prometheus                 | http://localhost:9090            | raw metrics & queries                          |
| Kafka console (Redpanda)   | http://localhost:8090            | topics, messages, headers, consumer lag        |
| Mock provider              | http://localhost:8081            | stand-in for the external provider             |

By default deliveries go to the bundled mock provider. To deliver to a real
endpoint instead, create a `.env` file:

```bash
echo 'PROVIDER_URL=https://webhook.site/<your-uuid>' > .env
docker compose up -d worker
```

---

## API

Full contract in [Swagger UI](http://localhost:8080/docs). The essentials:

| Method   | Path                          | Purpose                                  |
|----------|-------------------------------|------------------------------------------|
| `POST`   | `/api/v1/notifications`       | create one notification                  |
| `POST`   | `/api/v1/notifications/batch` | create up to 1,000 in one request        |
| `GET`    | `/api/v1/notifications`       | list, filter by status/channel/date, paginate |
| `GET`    | `/api/v1/notifications/:id`   | status & delivery details                |
| `DELETE` | `/api/v1/notifications/:id`   | cancel (only while not yet processing)   |
| `GET`    | `/api/v1/batches/:id`         | per-status summary of a batch            |

**Scheduled delivery** — pass a future `scheduled_at` and the scheduler
dispatches it when due:

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"a@b.com","channel":"email","content":"Reminder",
       "scheduled_at":"2030-01-01T09:00:00Z"}'
```

**Idempotency** — send an `Idempotency-Key` header to make retries safe.
Replays return the original record (`200` + `Idempotency-Replayed: true`);
reusing a key with a different payload is rejected with `409`:

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42-confirmation' \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Order confirmed"}'
```

Uniqueness is enforced by a partial unique index in PostgreSQL — not by
application code — so concurrent duplicates can never both insert.

---

## Architecture

Four independently built services share one Go module and one schema:

| Service         | Entry point        | Responsibility                                              |
|-----------------|--------------------|-------------------------------------------------------------|
| `api`           | `cmd/api`          | validation, persistence, publishing to Kafka, status queries |
| `worker`        | `cmd/worker`       | consuming, rate limiting, delivery, retries, DLQ             |
| `scheduler`     | `cmd/scheduler`    | dispatching due scheduled notifications + reconciliation reaper |
| `mock-provider` | `cmd/mockprovider` | self-contained stand-in for the external provider            |

### Notification lifecycle

The **database row is the single source of truth**; Kafka messages only carry
work, never state.

```
              ┌──────────────► cancelled
              │
 pending ──► queued ──► processing ──► sent
                ▲            │
 scheduled ─────┘ (when due) └──► dead  (retries exhausted → DLQ)
```

Every transition is a conditional `UPDATE` guarded by the current status, so
races (e.g. cancelling a message a worker just claimed) resolve atomically in
the database.

### Queue topology: channel × priority

Kafka has no native priorities and channels must not affect each other, so
both dimensions are encoded in the topic name:

```
sms.high    sms.normal    sms.low      sms.dlq
email.high  email.normal  email.low    email.dlq
push.high   push.normal   push.low     push.dlq
```

The worker runs **one fully isolated pool per channel** — its own consumers
(one per priority topic), its own sender goroutines, its own rate limiter.
A slow or failing channel can never starve the others.

Within a pool, senders drain queues in **strict priority order**: high before
normal before low, re-checked on every message.

### Delivery, retries and the DLQ

- **Rate limit**: a token bucket caps each channel at 100 deliveries/sec.
- **Retries**: transient failures (timeouts, 5xx, 429) retry in-memory with
  linear backoff; permanent failures (4xx) short-circuit immediately.
- **DLQ**: when retries are exhausted, the message is wrapped in an envelope
  (original payload + error + attempt count) and published to the channel's
  `.dlq` topic; the row is marked `dead`. The pipeline never blocks on a
  poison message.

Demo the failure path by making the mock provider flaky:

```bash
MOCK_FAIL_RATE=0.5 docker compose up -d mock-provider
```

Watch retries in the worker logs and dead-lettered envelopes in the Kafka
console under `sms.dlq` / `email.dlq` / `push.dlq`.

### Reliability model: at-least-once, deduplicated at the row

Delivery guarantees come from the database, not from Kafka offsets:

- **Claim before send** — a worker first executes a conditional
  `UPDATE ... WHERE status IN ('queued','pending')`. If the row was already
  sent, cancelled, or claimed by another worker, the claim fails and the
  message is skipped. This single statement is what makes duplicate
  deliveries and cancellation races harmless.
- **Commit after outcome** — Kafka offsets are committed only after the
  notification reached a terminal state (or the DLQ). A crash mid-delivery
  replays the message instead of losing it.
- **Publish failures are not fatal** — if the API persists a row but Kafka is
  down, the row simply stays `pending` and the response still succeeds.
- **The reaper closes every remaining gap.** A reconciliation poller
  re-dispatches rows stranded in a non-terminal state: `pending` rows that
  never reached Kafka, `queued`/`processing` rows lost to a crash or an
  out-of-order offset commit. Its in-flight window is floored at the
  worst-case delivery time (provider timeout × attempts + margin) so it can
  never reclaim — and double-send — a message still legitimately in flight.

The reaper is designed to stay idle. Its metric
(`notify_reaper_reaped_total`) going non-zero is an alert that something
upstream is stranding rows — a signal to fix the cause, not a load-bearing
component.

### Scheduler

A poller claims due rows with `FOR UPDATE SKIP LOCKED` (safe to run multiple
instances), publishes them while the rows are still locked, and only then
flips them to `queued` — a message is never marked queued without having
reached Kafka.

---

## Observability

### Dashboards & metrics

Grafana (http://localhost:3000) opens straight to the **go-notify** dashboard:
request rates and latency percentiles, notifications by channel/priority,
delivery outcomes, provider latency, queue depths, in-flight deliveries and
reaper activity. All services expose Prometheus metrics under a shared
`notify_*` namespace (API `:8080/metrics`, worker `:9100`, scheduler `:9110`).

### Distributed tracing

Every request is traced end-to-end with OpenTelemetry: the trace context
propagates from the API through Kafka message headers to the worker, so one
trace shows the entire journey —

```
POST /api/v1/notifications            99ms   notify-api
├─ INSERT notifications                19ms     (db)
├─ UPDATE status=queued                 6ms     (db)
└─ kafka.publish                       66ms
   └─ notification.process             80ms   notify-worker
      ├─ UPDATE status=processing      11ms     (db)
      ├─ HTTP POST  (provider)         24ms
      └─ UPDATE status=sent             4ms     (db)
```

Explore traces in **Grafana → Explore → Tempo** (e.g. query
`{ name = "POST /api/v1/notifications" }`). Spans are exported over OTLP to
Tempo; database queries and provider calls are instrumented automatically.
Operational endpoints (`/metrics`, `/healthz`) are excluded to keep traces
noise-free. Tracing is opt-in: without `OTEL_EXPORTER_OTLP_ENDPOINT` the
tracer is a no-op.

### Logs

Structured JSON via `log/slog`. Every log line carries the `correlation_id`
(accepted from the `X-Correlation-ID` header or generated) and the `trace_id`,
so you can pivot from a log line to its trace and back. The correlation id
rides Kafka headers across services and is echoed in API responses.

---

## Configuration

Everything is environment-driven with sensible defaults
(see [internal/config/config.go](internal/config/config.go) for the full list):

| Variable                      | Default                  | Purpose                                  |
|-------------------------------|--------------------------|-------------------------------------------|
| `HTTP_ADDR`                   | `:8080`                  | API listen address                         |
| `DATABASE_URL`                | local postgres DSN       | shared by all services                     |
| `KAFKA_BROKERS`               | `localhost:9092`         | comma-separated broker list                |
| `PROVIDER_URL`                | *(mock in compose)*      | external provider endpoint                 |
| `RATE_LIMIT_PER_SEC`          | `100`                    | per-channel delivery cap                   |
| `SENDER_CONCURRENCY`          | `16`                     | sender goroutines per channel pool         |
| `RETRY_MAX_ATTEMPTS`          | `3`                      | in-memory delivery attempts before DLQ     |
| `RETRY_BACKOFF`               | `500ms`                  | base backoff (linear)                      |
| `SCHEDULER_INTERVAL`          | `5s`                     | poll cadence for due notifications         |
| `REAPER_INTERVAL`             | `1m`                     | reconciliation sweep cadence               |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(empty = tracing off)*  | OTLP gRPC endpoint (Tempo)                 |
| `MOCK_FAIL_RATE`              | `0`                      | mock provider failure injection (0..1)     |

---

## Development

```bash
make build   # compile all services
make test    # run the test suite
make lint    # golangci-lint
make up      # docker compose up --build
make down    # docker compose down -v (wipes data)
```

Run a service directly against the compose infrastructure:

```bash
docker compose up -d postgres kafka kafka-init mock-provider
PROVIDER_URL=http://localhost:8081 go run ./cmd/worker
```

Image builds use BuildKit cache mounts for the Go module and build caches —
after the first build, code-only rebuilds take seconds.

## Testing

```bash
make test        # go test ./...
go test ./... -v # verbose
```

Tests run on the standard `testing` package with table-driven cases, using
[testify](https://github.com/stretchr/testify) (`require`/`assert`) for concise
assertions. They are pure unit tests — no database or broker required:

- **`domain`** — validation rules, per-channel content limits (rune-aware), the
  scheduled-vs-immediate status branch, and aggregated error reporting.
- **`api`** — the behaviors that matter most: idempotency replay (`200` +
  `Idempotency-Replayed`), key reuse with a different payload (`409`),
  validation (`422`) and cancel races (`409`). Handlers depend on a
  consumer-side `Repository` interface, so they are driven by a small
  hand-rolled fake instead of a live database.
- **`provider`** — retryable (5xx, 429) vs permanent (4xx) classification,
  exercised against an `httptest` server.
- **`worker`** — strict priority draining (high before normal before low) and
  clean shutdown on context cancellation.

Repository-layer SQL (idempotent insert, `DispatchDue`, `ReapStuck`) is
integration territory and is intentionally left for a Postgres-backed suite
rather than mocked.

## Repository layout

```
cmd/                  service entry points (api, worker, scheduler, mockprovider)
internal/
  domain/             core model, status state machine, validation
  storage/            PostgreSQL repository (claims, dispatch, reaping)
  queue/              Kafka topology, producer/consumer, DLQ, trace propagation
  provider/           external provider HTTP client
  api/                HTTP handlers, routing, middleware, OpenAPI spec
  worker/             per-channel pools, priority draining, retries
  scheduler/          scheduled dispatcher + reconciliation reaper
  observ/             OpenTelemetry tracing bootstrap
  config/             environment-based configuration
  metrics/            Prometheus collectors (shared notify_* namespace)
migrations/           versioned SQL migrations (embedded, applied on startup)
deploy/               Prometheus, Grafana and Tempo provisioning
build/                shared multi-stage Dockerfile
```

## Design trade-offs

Deliberate choices, made for this scope and easy to revisit:

- **At-least-once, not exactly-once.** Duplicates are possible by design and
  neutralized by the row claim. Exactly-once would require coordinated
  transactions across Kafka and PostgreSQL for marginal benefit here.
- **In-memory retries, not durable ones.** Backoffs are short, so retrying
  within the worker keeps the design simple. If long backoffs were needed,
  retries would move to the database (a retry state + due-time column) at the
  cost of a second dispatch path.
- **Per-process rate limiter.** Correct for one worker instance (the compose
  setup). Scaling workers horizontally would multiply the effective rate —
  the limit would then move to a shared store (e.g. a Redis token bucket) or
  be divided across replicas.
- **Strict priority, no fairness.** A sustained flood of high-priority
  messages can starve low. Acceptable per the spec; weighted draining or age
  promotion would be the fix if needed.
- **Sync publish inside the request.** The API waits for Kafka acks
  (~10ms batch linger), trading a little latency for a much simpler
  consistency story than an outbox table.
