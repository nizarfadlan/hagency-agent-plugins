# herdr-runtime

`hagency.runtime/v1` adapter for a Herdr-managed agent (OMP). The host starts it per dispatch; it prompts the target pane with `herdr agent prompt --wait` and returns the agent's answer. See the repository README for installation and settings.

## Host tools for the OMP pane (MCP bridge)

When the host offers task tools in `turn/start` (a runtime that advertises `tool`; the coordination tools also need `coordination_tools` and owner approvals configured on the host), the plugin serves them to the pane's OMP through a stdio MCP server in the same binary:

```
herdr-runtime --mcp --target <herdr target> [--bridge-dir <dir>]
```

Add it to the `mcp.json` OMP reads for that agent (one entry per agent profile, with `--target` set to that agent's Herdr target):

```json
{
  "mcpServers": {
    "hagency": {
      "type": "stdio",
      "command": "/absolute/path/to/bin/herdr-runtime",
      "args": ["--mcp", "--target", "assistant-coder"]
    }
  }
}
```

No `env` is needed and none is forwarded. Pass `--bridge-dir` only when the plugin setting `bridge_dir` is not the default.

How it works:

- While a turn that was offered tools runs, the plugin listens on `<bridge_dir>/<target>.sock` (directory `0700`, socket `0600`; default `<home>/.hagency/herdr-bridge`, setting `bridge_dir`). The socket is removed when the turn ends, is cancelled, or the plugin shuts down. A turn with no tools creates no socket.
- `tools/call` becomes one `tool/call` frame to the host; the host's `tool/result` is the MCP result (`isError` when the host refused or the tool failed). The host owns the catalog, owner approval, call-id deduplication and capability checks. A call that needs the owner can take minutes.
- With no active turn for the target, the call is refused with `isError` and nothing is sent or queued. A call cannot name another target, task, dispatch or capability.
- `tools/list` answers from the live turn, else from `<bridge_dir>/<target>.catalog.json`, written at each turn start with tools and kept after the turn. Without a cache the list is empty.
- While a bridge call waits, the plugin's `progress` heartbeat pauses, because the host reads no frames while it waits for the owner.

Warm-up: OMP lists MCP tools when it loads the server. After adding the entry, run one dispatch to that target so the catalog cache exists, then reload OMP's MCP servers (or restart the pane) so it lists the tools. The first live run depends on this; whether OMP re-lists tools on its own and its MCP call timeout are not verified here.

Residual risk: the plugin and the MCP child run as the same Unix user, so any process of that user can connect to the socket during a turn and make a call for that target. The host still gates every call (catalog, owner approval, duplicate call ids), but a same-user process is not isolated from the pane. Isolation between panes needs separate Unix users per agent.
