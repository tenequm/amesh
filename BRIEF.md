# Brief: amesh shared-storage MCP server (Go, MVP)

Act now; do not stop to ask questions or end your turn on a question. If something is
genuinely blocking, write `BLOCKED: <what, where>` at the top of ./report.md with
everything gathered so far below it, then end the turn.

## Mandatory reading first

Read and follow, in this order:
1. ./scratch/skills/go-dev/SKILL.md (and any files it references inside its directory)
2. ./scratch/skills/mcp-best-practices/SKILL.md (same)
These two skills govern code style, project hygiene, and MCP tool design. Where this
brief and the skills conflict on style, the skills win; on scope, this brief wins.
`scratch/` is reference material and is gitignored - never commit it.

## What to build

A single small Go binary, `amesh`, that serves five MCP tools over an S3-compatible
bucket. It is shared storage for a fleet of coding agents (they keep team docs and
research briefs in it). This is an MVP: make it correct and clean, do not gold-plate.

Repo layout (this is a fresh repo; mirror the layout style of a "packages/<name>"
monorepo, Go module self-contained under the package dir):

```
packages/amesh/
  go.mod            module github.com/tenequm/amesh
  cmd/amesh/main.go  <- ALL implementation lives in this one file (owner's explicit requirement)
Justfile            (repo root; recipes below)
README.md           (repo root; short: what it is, env vars, run examples)
```

Keep the implementation in that single main.go. If it grows past comfort, prefer
longer-but-clear over splitting into packages - the owner explicitly wants one simple
compiled Go file for now.

## SDK and dependencies

- MCP: the OFFICIAL Go SDK - github.com/modelcontextprotocol/go-sdk. Verify its
  current import path and API from its repo/docs before coding (network access is
  enabled for this run). Use its stdio transport and its Streamable HTTP server
  support.
- S3: github.com/aws/aws-sdk-go-v2 with a custom endpoint (S3-compatible stores:
  Hetzner object storage in prod, MinIO locally). Path-style addressing must work.
- Nothing else beyond stdlib unless a skill demands it.

## Tools

Five tools whose names, arguments, and output shapes deliberately mirror the native
tools coding agents already know (Claude Code's Read/Write/Edit/Glob/Grep). Each tool
description must say it operates on SHARED cross-agent storage (team docs, briefs),
not the local filesystem.

1. shared_read(path, offset?, limit?) -> file content with `cat -n` style line
   numbers; offset/limit select a line window.
2. shared_write(path, content) -> create or overwrite; auto-create parent "dirs"
   (keys are prefix-structured).
3. shared_edit(path, old_string, new_string, replace_all?) -> exact string
   replacement; error if old_string absent; error if not unique unless replace_all.
4. shared_glob(pattern) -> matching paths (doublestar-style patterns; `**` lists all),
   sorted by last-modified descending.
5. shared_grep(pattern, path?, case_insensitive?) -> Go regexp search across all
   objects (optionally under a path prefix), output `path:lineno:line` per match.
   The dataset is megabytes; listing and scanning all objects per call is fine.

Path hygiene on every tool: reject absolute paths, `..`, and empty paths; all paths
are relative to the configured bucket prefix.

Concurrency: a single in-process mutex (or per-path) around read-modify-write in
shared_edit so concurrent calls do not interleave. No distributed locking.

## Transports and config

Subcommands:
- `amesh stdio` - stdio MCP server (used by a laptop Claude Code with local creds).
- `amesh serve --bind <addr:port>` - Streamable HTTP MCP endpoint. Bearer-token auth:
  require `Authorization: Bearer $AMESH_TOKEN` on every request when AMESH_TOKEN is
  set; refuse to start without AMESH_TOKEN unless `--insecure` is passed.

Env config (document in README):
- AMESH_S3_ENDPOINT (e.g. https://nbg1.your-objectstorage.com or http://127.0.0.1:9000)
- AMESH_S3_BUCKET (e.g. amesh)
- AMESH_S3_PREFIX (optional key prefix, default empty)
- AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (standard)
- AMESH_TOKEN (serve mode)

## Local testing backend

Provide a trivially simple way to run an S3 backend locally: a Justfile recipe
`s3-local` that runs MinIO in docker (single container, local data dir or anonymous
volume, known access/secret keys), plus a recipe or documented env block that points
`amesh` at it. Recipes also for: `build` (current platform), `build-all`
(linux/amd64 + darwin/arm64 into dist/), and a `smoke` recipe that runs the e2e check
below against the local MinIO.

## Definition of done (verify before finishing)

1. `go vet ./...` clean; builds for darwin/arm64 and linux/amd64.
2. End-to-end against local MinIO: start `amesh serve` with a token, then exercise
   ALL FIVE tools through the MCP Streamable HTTP protocol (initialize, tools/list,
   tools/call) - scripted curl or a tiny Go test client is fine. Also verify: a
   request without the token is rejected; shared_edit fails on a non-unique
   old_string; `..` paths are rejected.
3. `amesh stdio` answers an initialize/tools-list round trip (pipe JSON-RPC in).
4. Commit everything (except gitignored scratch/, report.md, run.log, dist/) to the
   `main` branch with clear conventional-commit messages. Do NOT add any remote, do
   NOT push.
5. Write ./report.md: what was built, SDK version pinned, exact commands you ran for
   the e2e verification with their observed output (trimmed), example MCP client
   config snippets for (a) laptop stdio mode and (b) HTTP mode with token, binary
   sizes, and anything deferred.

Never print or write credentials anywhere; the MinIO test keys are throwaway and fine
to appear in the Justfile.
