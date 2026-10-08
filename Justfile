set shell := ["bash", "-euo", "pipefail", "-c"]
set dotenv-load

[private]
default:
    @just --list --unsorted

# Build the current platform into dist/amesh
[working-directory('packages/amesh')]
build:
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o ../../dist/amesh ./cmd/amesh

# Build the two release targets
[working-directory('packages/amesh')]
build-all:
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o ../../dist/amesh-linux-amd64 ./cmd/amesh
    CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags='-s -w' -o ../../dist/amesh-darwin-arm64 ./cmd/amesh

[working-directory('packages/amesh')]
fmt:
    golangci-lint fmt ./...

[working-directory('packages/amesh')]
fmt-check:
    golangci-lint fmt --diff ./...

[working-directory('packages/amesh')]
lint:
    golangci-lint run ./...

[working-directory('packages/amesh')]
test:
    go tool gotestsum --format testname -- -race -count=1 ./...

check: fmt-check lint test

# Run MinIO with anonymous data storage and throwaway local credentials
s3-local:
    #!/usr/bin/env bash
    set -euo pipefail
    docker build --tag amesh-minio-local - <<'DOCKERFILE'
    FROM golang:1.27.1 AS build
    RUN CGO_ENABLED=0 go install github.com/minio/minio@RELEASE.2025-10-15T17-29-55Z
    FROM debian:bookworm-slim
    COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
    COPY --from=build /go/bin/minio /usr/local/bin/minio
    ENTRYPOINT ["/usr/local/bin/minio"]
    DOCKERFILE
    docker run --rm --detach --name amesh-minio -p 127.0.0.1:9000:9000 -p 127.0.0.1:9001:9001 -e MINIO_ROOT_USER=amesh-local -e MINIO_ROOT_PASSWORD=amesh-local-secret -v /data amesh-minio-local server /data --console-address :9001

s3-stop:
    docker stop amesh-minio

# Run both transports and every tool against local MinIO; creates the local bucket
smoke: build
    #!/usr/bin/env bash
    set -euo pipefail
    export AMESH_S3_ENDPOINT=http://127.0.0.1:9000 AMESH_S3_BUCKET=amesh AMESH_S3_PREFIX=smoke
    export AWS_ACCESS_KEY_ID=amesh-local AWS_SECRET_ACCESS_KEY=amesh-local-secret AWS_REGION=us-east-1
    export AMESH_SMOKE_BINARY="{{ justfile_directory() }}/dist/amesh"
    cd packages/amesh
    go tool gotestsum --format standard-verbose -- -race -count=1 -timeout=3m -run TestSmoke -v ./cmd/amesh

[working-directory('packages/amesh')]
tidy:
    go mod tidy
    go mod verify

hooks:
    lefthook install
