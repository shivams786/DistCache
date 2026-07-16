.PHONY: build run test test-unit test-integration test-race lint proto docker-up docker-down resilience-test load-test

build:
	go build ./cmd/server

run:
	go run ./cmd/server

test:
	go test ./...

test-unit:
	go test ./internal/cache ./internal/hash ./internal/cluster

test-integration:
	go test ./internal/app

test-race:
	go test -race ./...

lint:
	go vet ./...

proto:
	@echo "proto/cache.proto documents the gRPC API; transport is implemented with a JSON gRPC codec for a compact portfolio build."

docker-up:
	docker compose up --build

docker-down:
	docker compose down

resilience-test:
	go run ./tools/resilience

load-test:
	go run ./tools/loadtest
