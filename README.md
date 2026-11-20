# amesh

`amesh` is the per-machine peer messaging daemon. Opted-in sessions use one MCP server. Mesh mode connects them to peers on the hub. Lane mode connects a child to its parent on the same machine.

## Build and check

Install [Go](https://go.dev/) 1.27.4, [just](https://github.com/casey/just), [golangci-lint](https://golangci-lint.run/) 2.15.0 and [Lefthook](https://github.com/evilmartians/lefthook). From the repository root run:

```sh
just hooks
just check
just build
./dist/amesh -version
```

## Commands

| Invocation | Purpose |
| --- | --- |
| `amesh` or `amesh daemon` | Run the per-machine daemon |
| `amesh mcp` | Pipe stdio to the daemon for mesh tools |
| `amesh mcp --parent ID` | Pipe stdio for a local lane child |
| `amesh hook` | Forward hook input on stdin |
| `amesh status` | Read daemon status over its Unix socket |
| `amesh enroll` | Enroll this machine |
| `amesh -version` | Print the release version, or `development` in local builds |

Flags override environment defaults. Global flags go before the subcommand. Daemon, hook, status and enroll also accept flags after the subcommand. Everything after `mcp` is passed through verbatim. Pipe commands use only the socket configuration.

| Flag | Environment | Default |
| --- | --- | --- |
| `-state-dir` | `AMESH_STATE_DIR` | `~/.local/state/amesh` |
| `-socket-path` | `AMESH_SOCKET_PATH` | `amesh.sock` in the resolved state directory |
| `-claude-config-dirs` | `AMESH_CLAUDE_CONFIG_DIRS` | `~/.claude` |
| `-exclude` | `AMESH_EXCLUDE` | No exclusion roots (empty) |
| `-machine-name` | `AMESH_MACHINE_NAME` | Empty before enrollment |
| `-hub-url` | `AMESH_HUB_URL` | [Fleet hub API](https://nanoclaw.opx.one/peer) |

Directory lists use a colon separator. Paths accept an absolute path or a leading `~/`. Sessions in any directory listed in `AMESH_CLAUDE_CONFIG_DIRS` can join the mesh. Set `AMESH_EXCLUDE` to refuse mesh pipes for sessions whose working directory is at or under an exclusion root. The hub URL includes the API prefix. Logs go to stderr as text in a terminal and JSON when redirected.

## Releases

A tag of the form `v1.2.3` publishes three archives on the [releases page](https://github.com/tenequm/amesh/releases). Each contains the executable `amesh` at its root. For example, `amesh_1.2.3_darwin_arm64.tar.gz` reports `1.2.3` with `-version`. The other targets are `linux_amd64` and `linux_arm64`. Verify the archives against `checksums.txt` with `shasum -a 256 -c checksums.txt`. Run `just release 1.2.3` to build the same assets locally in `dist/1.2.3/`.
