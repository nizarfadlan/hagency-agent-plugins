//! Runs the real adapter binary against a fake `herdr` under the conditions
//! the Hagency host imposes: an allowlisted environment without `HOME`, a
//! short idle timeout per frame, and cancel/shutdown while a turn runs.
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{Receiver, RecvTimeoutError, channel},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// The host reads turn frames with a 2 s idle timeout; leave margin.
const MAX_FRAME_GAP: Duration = Duration::from_millis(1_500);
const ACCEPT_WITHIN: Duration = Duration::from_millis(500);

const FAKE_HERDR: &str = r#"#!/bin/sh
# Fake herdr: records what the adapter sent, then behaves like the CLI.
printf '%s' "${HOME:-}" > "$FAKE_DIR/home"
case "$1 $2" in
  "agent prompt")
    printf '%s' "$4" > "$FAKE_DIR/prompt"
    sleep "${FAKE_SLEEP:-3}"
    echo '{"id":"cli:agent:prompt","result":{"type":"agent_prompted"}}'
    ;;
  "agent get")
    # FAKE_AGENT: ready | busy | missing | down (the server is not running).
    case "${FAKE_AGENT:-ready}" in
      ready) echo '{"id":"cli:agent:get","result":{"agent":{"name":"'"$3"'","interactive_ready":true}}}' ;;
      busy) echo '{"id":"cli:agent:get","result":{"agent":{"name":"'"$3"'","interactive_ready":false}}}' ;;
      missing) echo '{"error":{"code":"agent_not_found","message":"agent target '"$3"' not found"},"id":"cli:agent:get"}' >&2; exit 1 ;;
      down) echo '{"id":"cli:agent:get","error":{"code":"server_not_running","message":"no herdr server is running"}}' >&2; exit 1 ;;
    esac
    ;;
  "agent read")
    printf ' ████ banner ████\n Update Available\n\n %s\n\n Thinking about it\n\n PLUGIN_OK\n\n❯\n π · ⬢ model · 📁 workspace\n' "$(cat "$FAKE_DIR/prompt")"
    ;;
  *) echo "unexpected $*" >&2; exit 2 ;;
esac
"#;

struct Fixture {
    dir: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "herdr-runtime-conformance-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(dir.join("config")).unwrap();
        let herdr = dir.join("herdr");
        fs::write(&herdr, FAKE_HERDR).unwrap();
        fs::set_permissions(&herdr, fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(
            dir.join("config/settings.json"),
            json!({"values": {"herdr_bin": herdr, "herdr_env": true}}).to_string(),
        )
        .unwrap();
        Self { dir }
    }
    fn read(&self, name: &str) -> String {
        fs::read_to_string(self.dir.join(name)).unwrap_or_default()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

struct Session {
    child: Child,
    stdin: ChildStdin,
    frames: Receiver<(Instant, Value)>,
}

impl Session {
    /// Starts the adapter with the guardian's environment shape: cleared,
    /// then only `PATH` and the plugin variables (plus the fixture's knobs).
    fn start(fixture: &Fixture, sleep_secs: u64) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-runtime"))
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HAGENCY_PLUGIN_ID", "herdr-runtime")
            .env("HAGENCY_PLUGIN_CONFIG_DIR", fixture.dir.join("config"))
            .env("FAKE_DIR", &fixture.dir)
            .env("FAKE_SLEEP", sleep_secs.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, frames) = channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                let value: Value =
                    serde_json::from_str(&line).expect("stdout carries only protocol frames");
                if send.send((Instant::now(), value)).is_err() {
                    break;
                }
            }
        });
        let mut session = Self {
            child,
            stdin,
            frames,
        };
        session.send(json!({"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocol":"hagency.runtime/v1","capabilities":[]}}));
        let (_, ready) = session.next(Duration::from_secs(2));
        assert_eq!(ready["result"]["ready"], true);
        session
    }
    fn send(&mut self, frame: Value) -> Instant {
        writeln!(self.stdin, "{frame}").unwrap();
        self.stdin.flush().unwrap();
        Instant::now()
    }
    fn next(&self, wait: Duration) -> (Instant, Value) {
        match self.frames.recv_timeout(wait) {
            Ok(frame) => frame,
            Err(RecvTimeoutError::Timeout) => panic!("no frame within {wait:?}"),
            Err(RecvTimeoutError::Disconnected) => panic!("adapter closed stdout"),
        }
    }
    fn start_turn(&mut self, body: &str) -> Instant {
        let prompt = json!({
            "agent": {"name": "assistant-2"},
            "instruction": "generic participant guidance",
            "inbox": [{"message": {"body": format!("[assistant-2](https://matrix.to/#/@a:s) {body}")}}]
        });
        let sent = self.send(
            json!({"jsonrpc":"2.0","id":"turn-start","method":"turn/start","params":{
            "dispatchId":"dispatch","turnId":"turn-1","prompt":prompt.to_string(),
            "workspace":"/tmp","harnessContext":{"target":"assistant","ownership":"existing"}}}),
        );
        let (at, accepted) = self.next(Duration::from_secs(2));
        assert_eq!(accepted["id"], "turn-start");
        assert_eq!(accepted["result"]["accepted"], true);
        assert!(at - sent < ACCEPT_WITHIN, "accepted after {:?}", at - sent);
        at
    }
    /// Collects frames until `turn/result`, asserting the heartbeat contract.
    fn until_result(&self, since: Instant) -> (Value, Duration) {
        let mut last = since;
        let mut max_gap = Duration::ZERO;
        loop {
            let (at, frame) = self.next(Duration::from_secs(5));
            max_gap = max_gap.max(at - last);
            last = at;
            if frame["method"] == "turn/result" {
                return (frame["params"].clone(), max_gap);
            }
            assert_eq!(frame["method"], "progress", "unexpected frame {frame}");
        }
    }
    fn wait_exit(mut self, within: Duration) {
        drop(self.stdin);
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if self.child.try_wait().unwrap().is_some() {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        panic!("adapter did not exit within {within:?}");
    }
}

fn wait_for(path: &Path, within: Duration) {
    let deadline = Instant::now() + within;
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "{} never appeared",
            path.display()
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn turn_is_accepted_at_once_heartbeats_and_returns_the_clean_answer() {
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 3);
    let accepted = session.start_turn("Reply with exactly: PLUGIN_OK");
    let (result, max_gap) = session.until_result(accepted);
    assert!(
        max_gap <= MAX_FRAME_GAP,
        "silent for {max_gap:?} during the turn"
    );
    assert_eq!(result["outcome"], "completed", "{result}");
    assert_eq!(result["finalText"], "Thinking about it\n\nPLUGIN_OK");
    assert!(result["usage"].is_null(), "usage stays unknown");

    let prompt = fixture.read("prompt");
    assert!(
        prompt.contains("@assistant-2 Reply with exactly: PLUGIN_OK"),
        "{prompt}"
    );
    assert!(!prompt.contains("generic participant guidance"), "{prompt}");
    assert!(!fixture.read("home").is_empty(), "herdr ran without HOME");

    session.send(json!({"jsonrpc":"2.0","id":"bye","method":"shutdown","params":null}));
    let (_, bye) = session.next(Duration::from_secs(2));
    assert_eq!(bye["result"]["accepted"], true);
    session.wait_exit(Duration::from_secs(2));
}

#[test]
fn cancel_interrupts_a_running_turn() {
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 30);
    session.start_turn("long task");
    wait_for(&fixture.dir.join("prompt"), Duration::from_secs(3));
    let sent = session.send(json!({"jsonrpc":"2.0","id":"cancel","method":"turn/cancel","params":{"dispatchId":"dispatch","turnId":"turn-1"}}));
    let mut cancel_ack = false;
    let result = loop {
        let (at, frame) = session.next(Duration::from_secs(3));
        assert!(
            at - sent < Duration::from_secs(3),
            "cancel took {:?}",
            at - sent
        );
        if frame["id"] == "cancel" {
            assert_eq!(frame["result"]["accepted"], true);
            cancel_ack = true;
        } else if frame["method"] == "turn/result" {
            break frame["params"].clone();
        }
    };
    assert!(cancel_ack, "turn/cancel was answered");
    assert_eq!(result["outcome"], "interrupted", "{result}");
    session.send(json!({"jsonrpc":"2.0","id":"bye","method":"shutdown","params":null}));
    session.wait_exit(Duration::from_secs(2));
}

#[test]
fn shutdown_during_a_turn_stops_it() {
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 30);
    session.start_turn("long task");
    wait_for(&fixture.dir.join("prompt"), Duration::from_secs(3));
    let sent = session.send(json!({"jsonrpc":"2.0","id":"bye","method":"shutdown","params":null}));
    loop {
        let (at, frame) = session.next(Duration::from_secs(3));
        assert!(
            at - sent < Duration::from_secs(3),
            "shutdown took {:?}",
            at - sent
        );
        if frame["id"] == "bye" {
            assert_eq!(frame["result"]["accepted"], true);
            break;
        }
    }
    session.wait_exit(Duration::from_secs(2));
}

#[test]
fn a_second_turn_while_one_runs_is_refused() {
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 30);
    session.start_turn("long task");
    session.send(json!({"jsonrpc":"2.0","id":"again","method":"turn/start","params":{
        "dispatchId":"dispatch","turnId":"turn-2","prompt":"x","harnessContext":{"target":"assistant"}}}));
    loop {
        let (_, frame) = session.next(Duration::from_secs(2));
        if frame["id"] == "again" {
            assert_eq!(frame["error"]["code"], -32000, "{frame}");
            break;
        }
    }
    session.send(json!({"jsonrpc":"2.0","id":"bye","method":"shutdown","params":null}));
    session.wait_exit(Duration::from_secs(3));
}

/// Runs `herdr-runtime --probe` the way the host's readiness check does: the
/// guardian's environment shape, one JSON object on stdin, one on stdout.
fn probe(fixture: &Fixture, agent: &str, input: Value) -> Value {
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-runtime"))
        .arg("--probe")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HAGENCY_PLUGIN_ID", "herdr-runtime")
        .env("HAGENCY_PLUGIN_CONFIG_DIR", fixture.dir.join("config"))
        .env("FAKE_DIR", &fixture.dir)
        .env("FAKE_AGENT", agent)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.to_string().as_bytes())
        .unwrap();
    let started = Instant::now();
    let output = child.wait_with_output().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "the probe is bounded"
    );
    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).expect("one JSON object")
}

/// The probe answers ready only when the dispatch's target agent exists and
/// is ready for input; a stopped server, a missing target, a busy agent or no
/// configured target are each not ready, with Herdr's own reason.
#[test]
fn readiness_probe_reports_the_target_agent() {
    let fixture = Fixture::new();
    let input = json!({"dispatchId": "dispatch", "harnessTemplate": {"target": "assistant"}});
    assert_eq!(
        probe(&fixture, "ready", input.clone()),
        json!({"ready": true})
    );
    // Herdr is asked about the configured target, under a resolved home.
    assert!(
        !fixture.read("home").is_empty(),
        "the probe gives Herdr a HOME"
    );
    for (agent, reason) in [
        ("down", "Herdr: no herdr server is running"),
        ("missing", "Herdr: agent target assistant not found"),
        ("busy", "Herdr agent assistant is not ready for input"),
    ] {
        assert_eq!(
            probe(&fixture, agent, input.clone()),
            json!({"ready": false, "reason": reason}),
            "{agent}"
        );
    }
    for missing in [
        json!({}),
        json!({"harnessTemplate": null}),
        json!({"harnessTemplate": {"target": ""}}),
    ] {
        assert_eq!(
            probe(&fixture, "ready", missing.clone())["ready"],
            false,
            "{missing}"
        );
    }
}
