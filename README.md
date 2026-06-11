# go-notify

Event-driven notification system that processes and delivers messages through
multiple channels (SMS, Email, Push) with reliable delivery, retries and
real-time status tracking.

## Architecture

```
Client ──REST──▶ api ──▶ PostgreSQL (state)
                  │
                  └─────▶ Kafka ──▶ worker ──▶ external provider (webhook.site)
                            ▲          │
                            │          └──▶ PostgreSQL (status updates)
                        scheduler ◀── polls due scheduled / retry notifications
```

Three independently built and deployed services share a single Go module:

| Service     | Entry point          | Responsibility                                            |
|-------------|----------------------|-----------------------------------------------------------|
| `api`       | `cmd/api`            | REST API: create/batch/query/cancel/list notifications    |
| `worker`    | `cmd/worker`         | Consumes Kafka, rate limits, delivers to provider         |
| `scheduler` | `cmd/scheduler`      | Dispatches scheduled notifications and due retries        |

## Repository layout

```
cmd/                  service entry points (one main.go per service)
internal/
  domain/             core model, status state machine, validation
  storage/            PostgreSQL repository
  queue/              Kafka producer/consumer wrappers
  provider/           external provider HTTP client
  api/                HTTP handlers, routing, middleware
  worker/             consume loop, rate limiting, retry logic
  scheduler/          polling dispatcher
  config/             environment-based configuration
migrations/           versioned SQL migrations
build/                per-service Dockerfiles
docker-compose.yml    one-command local setup
Makefile              build / test / lint / run targets
```

## Quick start

```bash
docker-compose up
```

## Development

```bash
make build   # compile all services
make test    # run the test suite
make lint    # run golangci-lint
```

> Status: work in progress — setup instructions, API examples and design
> decisions will be documented as the implementation progresses.
