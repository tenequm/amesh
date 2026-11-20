export GOLANGCI_LINT_CACHE := justfile_directory() / ".golangci-cache"
set positional-arguments
set shell := ["bash", "-euo", "pipefail", "-c"]

[private]
default:
    @just --list --unsorted

build:
    CGO_ENABLED=0 go build -trimpath -o dist/amesh ./cmd/amesh

fmt:
    golangci-lint fmt ./...

fmt-check:
    golangci-lint fmt --diff ./...

lint:
    golangci-lint run ./...

test:
    go tool gotestsum --format testname -- -race ./...

check: fmt-check lint test

hooks:
    lefthook install

# Build the fleet archives from a version without its tag prefix
release version:
    #!/usr/bin/env bash
    set -euo pipefail
    version="$1"
    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
    out="dist/$version"
    mkdir -p "$out"
    archives=()
    for target in darwin/arm64 linux/amd64 linux/arm64; do
        os="${target%/*}"
        arch="${target#*/}"
        archive="amesh_${version}_${os}_${arch}.tar.gz"
        CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath -ldflags="-s -w -X github.com/tenequm/amesh/internal/version.Value=$version" -o "$out/amesh" ./cmd/amesh
        COPYFILE_DISABLE=1 tar -C "$out" -czf "$out/$archive" amesh
        archives+=("$archive")
    done
    cd "$out"
    rm amesh
    shasum -a 256 "${archives[@]}" > checksums.txt
