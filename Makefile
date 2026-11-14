.PHONY: build test vet lint run-loadtest up down

build:
	go build -o bin/ ./cmd/...

vet:
	go vet ./...

lint:
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...

# Runs against an embedded Postgres and in-process Redis unless
# TEST_DATABASE_URL / TEST_REDIS_URL are set.
test:
	go test -race -count=1 -coverpkg=./... -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

run-loadtest:
	go run ./cmd/loadtest -workers 4,16,64 -items 1000

up:
	docker compose up --build -d

down:
	docker compose down -v
