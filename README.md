# amesh

A small Go [MCP](https://modelcontextprotocol.io/) server for shared team docs and
research briefs in an S3-compatible bucket. Five tools mirror familiar agent file
tools: `shared_read`, `shared_write`, `shared_edit`, `shared_glob`, `shared_grep`.
They operate on shared cross-agent storage. Implementation lives in
`packages/amesh/cmd/amesh/main.go` and uses the
[official Go SDK](https://github.com/modelcontextprotocol/go-sdk).

## Configuration

| Environment variable | Meaning |
| --- | --- |
| `AMESH_S3_ENDPOINT` | Required S3 URL, e.g. `https://nbg1.your-objectstorage.com` |
| `AMESH_S3_BUCKET` | Required existing bucket, e.g. `amesh` |
| `AMESH_S3_PREFIX` | Optional relative key prefix; default empty; a trailing slash is allowed |
| `AWS_ACCESS_KEY_ID` | Standard AWS access key |
| `AWS_SECRET_ACCESS_KEY` | Standard AWS secret key |
| `AWS_SESSION_TOKEN` | Optional standard AWS session token |
| `AWS_REGION` | Optional signing region; default `us-east-1` |
| `AMESH_TOKEN` | Required bearer token for `serve`, unless `--insecure` is supplied |

The AWS SDK's standard credential chain also works. Path-style addressing is
enabled for S3-compatible stores. Keep credentials in the environment or your
credential store. The server does not log them.

## Build and run

Requires Go 1.27+, [just](https://github.com/casey/just), and
[Docker](https://www.docker.com/) for the local backend.

```sh
just build
./dist/amesh stdio

# With AMESH_TOKEN set in the environment:
./dist/amesh serve --bind 127.0.0.1:8080

# Trusted local development only:
./dist/amesh serve --bind 127.0.0.1:8080 --insecure

just build-all
```

HTTP clients connect to `/mcp`; send `Authorization: Bearer <token>` on every
request. Stateless Streamable HTTP supports both the 2025-11-25 initialization
flow and the 2026-07-28 discovery flow. GET and DELETE return 405 after authentication.
Missing or invalid tokens return 401. Present Origin headers must match the
request host. For remote use, terminate HTTPS at a reverse proxy and preserve Host.

## Local backend and verification

```sh
just s3-local
just smoke
just s3-stop
```

`s3-local` runs one [MinIO](https://github.com/minio/minio) container with an
anonymous data volume and throwaway credentials. Stopping it removes the data.
The recipe builds a pinned source release because upstream community images are
no longer available; the first build takes several minutes and subsequent runs
use Docker's cache.
The smoke test creates the local bucket, uses an isolated prefix, launches
`amesh serve` with a generated token, and exercises every tool through raw
JSON-RPC over HTTP in both protocol eras. It also checks stdio, unauthenticated
requests, invalid inputs, and concurrent edits, then removes its objects.

For interactive local use, create bucket `amesh` in the
[MinIO console](http://127.0.0.1:9001) or run `just smoke` once, then set:

```sh
export AMESH_S3_ENDPOINT=http://127.0.0.1:9000
export AMESH_S3_BUCKET=amesh
export AMESH_S3_PREFIX=team
export AWS_ACCESS_KEY_ID=amesh-local
export AWS_SECRET_ACCESS_KEY=amesh-local-secret
export AWS_REGION=us-east-1
./dist/amesh stdio
```

`just check` formats-checks, lints, and runs unit tests with the race detector.
Install [golangci-lint](https://golangci-lint.run/) v2.14.0 and
[lefthook](https://github.com/evilmartians/lefthook) v2.1.17+; `just hooks` enables
the commit checks. Test and formatter tools are pinned in the module.

## Tool behavior

- Paths use forward slashes and are relative to `AMESH_S3_PREFIX`. Empty paths,
  absolute paths, drive letters, dot segments, empty segments, backslashes, and
  NUL/tab/newline characters are rejected. Directory markers are excluded.
- `shared_read` uses a 1-based `offset` (default 1) and a positive `limit`
  (default all remaining lines). Empty files and windows beyond EOF return empty text.
- `shared_write` overwrites the whole object; parent prefixes need no creation.
- `shared_edit` requires non-empty exact old text. Ambiguous matches fail,
  including overlapping matches. `replace_all` replaces non-overlapping occurrences.
- `shared_glob` supports `*`, `?`, Go character classes (negation `[^...]`), and
  `**` as a complete segment matching zero or more path segments. Results are
  newest first, with lexical order for timestamp ties.
- `shared_grep` uses [Go regexp syntax](https://pkg.go.dev/regexp/syntax) per line.
  Omit `path` to search all objects; a supplied path selects a file or directory
  at a segment boundary. Output is `path:lineno:line`, sorted by path.

Results use MCP text content; recoverable failures use `isError: true`.
Shared files are limited to 16 MiB; oversized reads, writes, and edit expansions
return tool errors. HTTP request bodies are limited to 8 MiB, including JSON overhead.
Listing is paginated internally. Grep scans all selected objects, intended for
megabytes of text. Writes and edits share one process mutex; separate processes
can overwrite each other's changes. There is no distributed locking or version history.
