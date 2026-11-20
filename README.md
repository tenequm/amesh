# amesh

`amesh` lets Claude Code sessions message each other. One daemon runs per machine. Each Claude Code session talks to it through a small MCP server (`amesh mcp`), and peer messages arrive in the session as native cross-session messages.

It has two modes:

- **Lane mode** works on one machine with no other setup. A session whose MCP server runs as `amesh mcp --parent <session id>` gets one tool, `send_to_parent`, and the session that launched it answers with a native `SendMessage`.
- **Mesh mode** connects sessions to a hub, so they can find and message agents and Claude Code sessions on other machines with the `list_peers` and `send_message` tools. The hub is a [nanoclaw](https://github.com/tenequm/nanoclaw) host with its peer channel enabled ([docs/peer-channel.md](https://github.com/tenequm/nanoclaw/blob/main/docs/peer-channel.md)). The wire contract is [api/openapi.yaml](api/openapi.yaml).

amesh runs on macOS and Linux and needs a Claude Code version with cross-session messaging.

## Install

Download the archive for your platform and `checksums.txt` from the [releases page](https://github.com/tenequm/amesh/releases), then verify and install it:

```sh
shasum -a 256 -c checksums.txt --ignore-missing
tar -xzf amesh_1.2.3_darwin_arm64.tar.gz
install -m 0755 amesh ~/.local/bin/amesh
amesh -version
```

Archives exist for `darwin_arm64`, `linux_amd64` and `linux_arm64`. Each contains the executable `amesh` at its root. To build from source instead, see [Build and check](#build-and-check).

## Run the daemon

Run `amesh` (or `amesh daemon`) as a long-lived user service, for example a launchd agent on macOS or a systemd user unit on Linux. It keeps its state in `~/.local/state/amesh` and listens on a Unix socket there. Logs go to stderr, as text in a terminal and as JSON when redirected.

`amesh status` prints the daemon's state as JSON: the hub connection, the sessions it hosts, the lanes and the watched config directories.

## Connect Claude Code

Add the MCP server and two hooks to each Claude Code config directory that should use amesh:

```sh
claude mcp add --scope user amesh -- amesh mcp
```

In that config directory's `settings.json`:

```json
{
  "crossSessionInbound": "accept",
  "permissions": { "allow": ["mcp__amesh__*"] },
  "hooks": {
    "PreToolUse": [
      { "matcher": "mcp__amesh__.*", "hooks": [{ "type": "command", "command": "amesh hook" }] }
    ],
    "UserPromptSubmit": [
      { "hooks": [{ "type": "command", "command": "amesh hook", "async": true }] }
    ]
  }
}
```

The `PreToolUse` hook keeps the mesh tools away from subagents. The `UserPromptSubmit` hook tells the daemon when a person typed in a session, which resets the hop limit of that session's conversations. If you run Claude Code with more than one config directory (`CLAUDE_CONFIG_DIR`), list them all in `AMESH_CLAUDE_CONFIG_DIRS`.

## Point it at a hub

Without a hub URL the daemon runs lane mode only, and `list_peers` and `send_message` answer that no hub is configured. To join a mesh:

1. Pick a machine name (lowercase letters, digits and `-`, at most 16 characters) and enroll:

   ```sh
   AMESH_MACHINE_NAME=laptop amesh enroll
   ```

   This creates the machine key in the state directory and prints the command the hub owner runs on the hub, `ncl peers add --name laptop --public-key <key>`.

2. Start the daemon with the machine name and the hub's API base, including the `/peer` prefix:

   ```sh
   AMESH_MACHINE_NAME=laptop AMESH_HUB_URL=https://hub.example.com/peer amesh daemon
   ```

Keep the hub on a private network. Every request is signed with the machine key (the `Amesh-Sig` scheme in the contract).

## Configuration

Flags override environment variables. Global flags go before the subcommand. `daemon`, `hook`, `status` and `enroll` also accept flags after the subcommand. Everything after `mcp` is passed through verbatim. Pipe commands use only the socket configuration.

| Flag | Environment | Default |
| --- | --- | --- |
| `-state-dir` | `AMESH_STATE_DIR` | `~/.local/state/amesh` |
| `-socket-path` | `AMESH_SOCKET_PATH` | `amesh.sock` in the state directory |
| `-claude-config-dirs` | `AMESH_CLAUDE_CONFIG_DIRS` | `~/.claude` |
| `-exclude` | `AMESH_EXCLUDE` | No exclusion roots |
| `-machine-name` | `AMESH_MACHINE_NAME` | Empty until you enroll |
| `-hub-url` | `AMESH_HUB_URL` | Empty: no hub, lane mode only |

Directory lists use a colon separator. Paths accept an absolute path or a leading `~/`. Sessions in any directory listed in `AMESH_CLAUDE_CONFIG_DIRS` can join the mesh. Set `AMESH_EXCLUDE` to refuse mesh pipes for sessions whose working directory is at or under one of its roots; lane pipes are not affected.

## Commands

| Invocation | Purpose |
| --- | --- |
| `amesh` or `amesh daemon` | Run the per-machine daemon |
| `amesh mcp` | Pipe stdio to the daemon for the mesh tools |
| `amesh mcp --parent ID` | Pipe stdio for a lane child of session `ID` |
| `amesh hook` | Forward hook input on stdin |
| `amesh status` | Print daemon status as JSON |
| `amesh enroll` | Create the machine key and print the hub enrollment command |
| `amesh -version` | Print the release version, or `development` in local builds |

## Build and check

Install [Go](https://go.dev/) 1.27.4, [just](https://github.com/casey/just), [golangci-lint](https://golangci-lint.run/) 2.15.0, [Lefthook](https://github.com/evilmartians/lefthook) and [Betterleaks](https://github.com/betterleaks/betterleaks) 1.10. From the repository root run:

```sh
just hooks
just check
just build
./dist/amesh -version
```

The Git hooks also scan what you commit and push for secrets and private data (`just leaks`).

## Releases

A tag of the form `v1.2.3` publishes the three archives and `checksums.txt` on the [releases page](https://github.com/tenequm/amesh/releases). `amesh_1.2.3_darwin_arm64.tar.gz` reports `1.2.3` with `-version`. Run `just release 1.2.3` to build the same assets locally in `dist/1.2.3/`.
