# Hagency plugin examples

Small plugin packages for the Hagency boundaries:

- `reference-harness` implements the generic `hagency.harness/v1` seam for the
  next agent-harness design.
- `herdr-runtime` implements `hagency.runtime/v1` as the current Herdr adapter.

The split is intentional: the harness is generic and view-only, while the runtime
adapter is the provider-specific execution side. No package contains Matrix,
operator, provider, or runner credentials.

## What each package proves

### Reference harness

`reference.harness` is a view-only harness. It accepts the host lifecycle messages
`initialize`, `snapshot`, `event`, `agent/prepare`, `agent/release`, and `shutdown`.
`agent/prepare` validates and echoes the template's opaque target and ownership;
it does not launch or prompt a process. Release is bound to the same allocation,
dispatch, and target.

The harness has no turn, approval, tool, usage, or task-completion authority. The
future agent shim/VM boundary belongs behind a separately qualified runtime
adapter; it should not be added to this fixture.

### Herdr runtime

`herdr-runtime` consumes the target returned by `agent/prepare` through
`turn/start.params.harnessContext.target`, then invokes Herdr's `agent prompt` and
`agent read` commands. It does not choose a replacement target and does not own
approval, usage, task state, cleanup, or settlement.

Herdr remains responsible for its own provider login and credential home. The
adapter only receives the executable setting and the host-supplied harness target;
no credential value is checked in here.

## Build and smoke test

Run from this repository root:

```sh
cargo fmt --all --check
cargo build --workspace --locked
cargo test --workspace --locked
```

Build either package directly when preparing a package-local binary:

```sh
cargo build --release --locked --manifest-path packages/reference-harness/Cargo.toml
cargo build --release --locked --manifest-path packages/herdr-runtime/Cargo.toml
```

The package manifests live under `packages/`. The harness has no settings
credential surface. The runtime exposes only `herdr_bin` and the explicit
`herdr_env` opt-in; provider credentials stay outside this repository.

The package launchers are intentionally POSIX-only (`linux-x86_64` in the
manifests). They prefer a package-local release binary, then a package-local debug
binary, and finally the workspace binary, so GitHub package builds and local
linked-plugin smoke tests use the same entrypoint.
