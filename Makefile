.PHONY: build test lint run-api run-worker run-scheduler up down

build:
	go build ./...

test:
	go test ./...

lint:
	golangci-lint run

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

run-scheduler:
	go run ./cmd/scheduler

up:
	docker-compose up --build

down:
	docker-compose down -v
