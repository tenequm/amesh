# amesh

Use `just fmt` to format and `just check` to run the formatter check, lint and race tests. Keep stdout reserved for the subcommand's output.

The source of [api/openapi.yaml](api/openapi.yaml) is `~/Projects/infra/docs/plans/2611-19-peer-messaging-hub/lanes/openapi.yaml`. Change the source first and copy it byte for byte. The adjacent `contract.md` defines behavior that schemas cannot express.

| Lane | Owned packages under `internal/` |
| --- | --- |
| amesh-wire | `envelope`, `relay`, `state` |
| amesh-identity | `registry`, `control`, `mcpserver` |
| amesh-hub | `hub`, `contract` |
| amesh-lanemode | `lane` |
| amesh-mesh | `mesh` |

The mode backends implement the interfaces in `mcpserver`. Each has one constructor in the daemon wiring. Keep lane communication local. Take connection identity from `peercred.Read`, and re-resolve the session on each call.

Use `testing/synctest` for timeouts and backoff, short Unix socket paths for fake inboxes, and a recording `httptest` hub for HTTP. A lost response can be simulated with `panic(http.ErrAbortHandler)`.
