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
            json!({"values": {"herdr_bin": herdr, "herdr_env": true, "bridge_dir": dir.join("bridge")}})
                .to_string(),
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
        Self::start_with(fixture, sleep_secs, &[])
    }
    fn start_with(fixture: &Fixture, sleep_secs: u64, extra: &[(&str, &str)]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-runtime"))
            .env_clear()
            .envs(extra.iter().copied())
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
        self.start_turn_with(body, "turn-1", None)
    }
    fn start_turn_with(&mut self, body: &str, turn_id: &str, tools: Option<Value>) -> Instant {
        let prompt = json!({
            "agent": {"name": "assistant-2"},
            "instruction": "generic participant guidance",
            "inbox": [{"message": {"body": format!("[assistant-2](https://matrix.to/#/@a:s) {body}")}}]
        });
        let mut params = json!({
            "dispatchId":"dispatch","turnId":turn_id,"prompt":prompt.to_string(),
            "workspace":"/tmp","harnessContext":{"target":"assistant","ownership":"existing"}});
        if let Some(tools) = tools {
            params["tools"] = tools;
        }
        let sent = self
            .send(json!({"jsonrpc":"2.0","id":"turn-start","method":"turn/start","params":params}));
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
    // The host's real input: the prepared `harnessContext` the turn will get,
    // beside the frozen template values under the harness's own key names.
    let input = json!({
        "dispatchId": "dispatch",
        "harnessContext": {"target": "assistant", "ownership": "existing"},
        "harnessTemplate": {"reference_target": "assistant", "reference_ownership": "existing"},
    });
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
        json!({"harnessContext": null, "harnessTemplate": null}),
        json!({"harnessContext": {"target": ""}}),
        // A template alone is not a target: only the prepared context is.
        json!({"harnessTemplate": {"reference_target": "assistant"}}),
        json!({"harnessTemplate": {"target": "assistant"}}),
    ] {
        assert_eq!(
            probe(&fixture, "ready", missing.clone())["ready"],
            false,
            "{missing}"
        );
    }
}

// ------------------------------------------------------------------ bridge

const DECOY: &str = "decoy-runner-capability-value";

fn tools() -> Value {
    json!([
        {"name":"delegate_task","description":"Delegate a task","readOnly":false,
         "inputSchema":{"type":"object","properties":{"assignee":{"type":"string"}}}},
        {"name":"read_peer_inbox","description":"Read the inbox","readOnly":true,
         "inputSchema":{"type":"object"}}
    ])
}

fn sock(fixture: &Fixture) -> PathBuf {
    fixture.dir.join("bridge/assistant.sock")
}

fn catalog(fixture: &Fixture) -> PathBuf {
    fixture.dir.join("bridge/assistant.catalog.json")
}

/// `herdr-runtime --mcp`, the way OMP starts it: cleared environment, stdio.
struct Mcp {
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<String>,
    ids: u64,
}

impl Mcp {
    fn start(fixture: &Fixture, target: &str) -> Self {
        Self::start_with(fixture, &["--target", target], &[])
    }
    fn start_with(fixture: &Fixture, args: &[&str], extra: &[(&str, &str)]) -> Self {
        let bridge = fixture.dir.join("bridge");
        let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-runtime"))
            .arg("--mcp")
            .args(args)
            .arg("--bridge-dir")
            .arg(&bridge)
            .env_clear()
            .envs(extra.iter().copied())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, lines) = channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            stdin,
            lines,
            ids: 0,
        }
    }
    fn send(&mut self, method: &str, params: Value) -> u64 {
        self.ids += 1;
        let id = self.ids;
        writeln!(
            self.stdin,
            "{}",
            json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
        )
        .unwrap();
        self.stdin.flush().unwrap();
        id
    }
    fn read(&self, wait: Duration) -> Value {
        match self.lines.recv_timeout(wait) {
            Ok(line) => serde_json::from_str(&line).expect("MCP stdout carries only JSON-RPC"),
            Err(_) => panic!("no MCP answer within {wait:?}"),
        }
    }
    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.send(method, params);
        let answer = self.read(Duration::from_secs(5));
        assert_eq!(answer["id"], id, "{answer}");
        answer
    }
    fn call(&mut self, tool: &str, arguments: Value) -> u64 {
        self.send("tools/call", json!({"name": tool, "arguments": arguments}))
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn text_of(answer: &Value) -> String {
    answer["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

fn tool_result(call_id: &str, result: Value) -> Value {
    json!({"jsonrpc":"2.0","id":format!("tool-{call_id}"),"method":"tool/result","params":{
        "dispatchId":"dispatch","turnId":"turn-1","callId":call_id,"result":result}})
}

impl Session {
    /// The next `tool/call` frame; progress frames are skipped.
    fn next_tool_call(&self) -> Value {
        loop {
            let (_, frame) = self.next(Duration::from_secs(5));
            match frame["method"].as_str() {
                Some("tool/call") => return frame["params"].clone(),
                Some("progress") => {}
                _ => panic!("unexpected frame {frame}"),
            }
        }
    }
    /// Every frame up to `turn/result`, which is returned last.
    fn drain(&self) -> Vec<Value> {
        let mut frames = Vec::new();
        loop {
            let (_, frame) = self.next(Duration::from_secs(8));
            let done = frame["method"] == "turn/result";
            frames.push(frame);
            if done {
                return frames;
            }
        }
    }
    fn expect_silence(&self, wait: Duration) {
        if let Ok((_, frame)) = self.frames.recv_timeout(wait) {
            panic!("unexpected frame {frame}");
        }
    }
}

fn count(frames: &[Value], method: &str) -> usize {
    frames.iter().filter(|f| f["method"] == method).count()
}

#[test]
fn bridge_relays_a_tool_call_during_an_active_turn() {
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 4);
    session.start_turn_with("delegate it", "turn-1", Some(tools()));
    wait_for(&sock(&fixture), Duration::from_secs(3));
    assert_eq!(
        fs::metadata(sock(&fixture)).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(fixture.dir.join("bridge"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );

    let mut mcp = Mcp::start(&fixture, "assistant");
    let init = mcp.request("initialize", json!({"protocolVersion":"2025-06-18"}));
    assert!(
        init["result"]["instructions"]
            .as_str()
            .unwrap()
            .contains("engagement ID")
    );
    let listed = mcp.request("tools/list", json!({}));
    let names: Vec<&str> = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["delegate_task", "read_peer_inbox"]);

    let id = mcp.call("delegate_task", json!({"assignee":"eng-1"}));
    let call = session.next_tool_call();
    assert_eq!(call["tool"], "delegate_task");
    assert_eq!(call["input"], json!({"assignee":"eng-1"}));
    assert_eq!(call["dispatchId"], "dispatch");
    assert_eq!(call["turnId"], "turn-1");
    let call_id = call["callId"].as_str().unwrap().to_owned();
    assert!(call_id.starts_with("omp-turn-1-"), "{call_id}");
    session.send(tool_result(
        &call_id,
        json!({"ok":true,"output":"delegated"}),
    ));
    let answer = mcp.read(Duration::from_secs(5));
    assert_eq!(answer["id"], id);
    assert_eq!(answer["result"]["isError"], false, "{answer}");
    assert_eq!(text_of(&answer), "delegated");

    // A refusal from the host is an MCP error with the host's own text.
    let id = mcp.call("read_peer_inbox", json!({}));
    let call = session.next_tool_call();
    let second = call["callId"].as_str().unwrap().to_owned();
    assert_ne!(second, call_id);
    session.send(tool_result(
        &second,
        json!({"ok":false,"error":"refused by the owner; nothing was sent"}),
    ));
    let answer = mcp.read(Duration::from_secs(5));
    assert_eq!(answer["id"], id);
    assert_eq!(answer["result"]["isError"], true, "{answer}");
    assert_eq!(text_of(&answer), "refused by the owner; nothing was sent");

    // A name outside the offered catalog never becomes a frame.
    mcp.call("create_graph", json!({}));
    let answer = mcp.read(Duration::from_secs(5));
    assert_eq!(answer["result"]["isError"], true, "{answer}");
    assert!(text_of(&answer).contains("not offered"), "{answer}");

    // Exactly one frame per accepted call.
    let frames = session.drain();
    assert_eq!(count(&frames, "tool/call"), 0, "{frames:?}");
    assert_eq!(frames.last().unwrap()["params"]["outcome"], "completed");
}

#[test]
fn bridge_refuses_without_an_active_turn() {
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 2);
    let mut mcp = Mcp::start(&fixture, "assistant");
    mcp.request("initialize", json!({}));
    mcp.call("delegate_task", json!({"assignee":"eng-1"}));
    let answer = mcp.read(Duration::from_secs(5));
    assert_eq!(answer["result"]["isError"], true, "{answer}");
    let text = text_of(&answer);
    assert!(
        text.contains("no active Hagency turn for target assistant")
            && text.contains("nothing was sent"),
        "{text}"
    );
    session.expect_silence(Duration::from_millis(400));
    // Nothing was queued: the next turn starts with no pending call.
    session.start_turn_with("later", "turn-1", Some(tools()));
    let frames = session.drain();
    assert_eq!(count(&frames, "tool/call"), 0, "{frames:?}");
}

#[test]
fn bridge_is_removed_when_the_turn_ends_or_is_cancelled() {
    // A turn that ends by itself.
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 1);
    session.start_turn_with("short", "turn-1", Some(tools()));
    wait_for(&sock(&fixture), Duration::from_secs(3));
    let frames = session.drain();
    assert_eq!(frames.last().unwrap()["params"]["outcome"], "completed");
    assert!(!sock(&fixture).exists(), "socket outlived the turn");
    let mut mcp = Mcp::start(&fixture, "assistant");
    mcp.call("delegate_task", json!({}));
    assert_eq!(mcp.read(Duration::from_secs(5))["result"]["isError"], true);
    session.send(json!({"jsonrpc":"2.0","id":"bye","method":"shutdown","params":null}));
    session.wait_exit(Duration::from_secs(2));

    // A cancelled turn also answers the call that is waiting.
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 30);
    session.start_turn_with("long", "turn-1", Some(tools()));
    wait_for(&sock(&fixture), Duration::from_secs(3));
    let mut mcp = Mcp::start(&fixture, "assistant");
    mcp.call("delegate_task", json!({}));
    session.next_tool_call();
    session.send(json!({"jsonrpc":"2.0","id":"cancel","method":"turn/cancel","params":{"dispatchId":"dispatch","turnId":"turn-1"}}));
    let answer = mcp.read(Duration::from_secs(5));
    assert_eq!(answer["result"]["isError"], true, "{answer}");
    assert!(text_of(&answer).contains("cancelled"), "{answer}");
    let frames = session.drain();
    assert_eq!(frames.last().unwrap()["params"]["outcome"], "interrupted");
    assert!(!sock(&fixture).exists(), "socket outlived the cancel");
    session.send(json!({"jsonrpc":"2.0","id":"bye","method":"shutdown","params":null}));
    session.wait_exit(Duration::from_secs(2));

    // Shutdown during a turn.
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 30);
    session.start_turn_with("long", "turn-1", Some(tools()));
    wait_for(&sock(&fixture), Duration::from_secs(3));
    session.send(json!({"jsonrpc":"2.0","id":"bye","method":"shutdown","params":null}));
    session.wait_exit(Duration::from_secs(4));
    assert!(!sock(&fixture).exists(), "socket outlived shutdown");
}

#[test]
fn bridge_is_not_started_when_the_host_offers_no_tools() {
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 2);
    session.start_turn_with("plain", "turn-1", None);
    wait_for(&fixture.dir.join("prompt"), Duration::from_secs(3));
    assert!(!sock(&fixture).exists());
    assert!(!catalog(&fixture).exists());
    session.drain();
    session.start_turn_with("empty", "turn-2", Some(json!([])));
    wait_for(&fixture.dir.join("prompt"), Duration::from_secs(3));
    assert!(!sock(&fixture).exists());
    assert!(!catalog(&fixture).exists());
    session.drain();
}

#[test]
fn bridge_never_exposes_the_runner_capability() {
    let fixture = Fixture::new();
    let env = [("HAGENCY_RUNNER_CAPABILITY", DECOY)];
    let mut session = Session::start_with(&fixture, 3, &env);
    session.start_turn_with("delegate it", "turn-1", Some(tools()));
    wait_for(&sock(&fixture), Duration::from_secs(3));
    let mut mcp = Mcp::start_with(&fixture, &["--target", "assistant"], &env);
    let mut seen = vec![
        mcp.request("initialize", json!({})).to_string(),
        mcp.request("tools/list", json!({})).to_string(),
    ];
    mcp.call("delegate_task", json!({}));
    let call = session.next_tool_call();
    seen.push(call.to_string());
    session.send(tool_result(
        call["callId"].as_str().unwrap(),
        json!({"ok":true,"output":"done"}),
    ));
    seen.push(mcp.read(Duration::from_secs(5)).to_string());
    seen.push(fs::read_to_string(catalog(&fixture)).unwrap());
    seen.push(sock(&fixture).display().to_string());
    seen.extend(session.drain().iter().map(Value::to_string));
    for text in seen {
        assert!(
            !text.contains("HAGENCY_RUNNER") && !text.contains(DECOY),
            "{text}"
        );
    }
}

#[test]
fn bridge_is_scoped_to_its_target() {
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 3);
    session.start_turn_with("delegate it", "turn-1", Some(tools()));
    wait_for(&sock(&fixture), Duration::from_secs(3));
    let mut other = Mcp::start(&fixture, "other");
    assert_eq!(
        other.request("tools/list", json!({}))["result"]["tools"],
        json!([])
    );
    other.call("delegate_task", json!({}));
    let answer = other.read(Duration::from_secs(5));
    assert_eq!(answer["result"]["isError"], true, "{answer}");
    assert!(
        text_of(&answer).contains("no active Hagency turn for target other"),
        "{answer}"
    );
    // A target cannot name a path outside the bridge directory.
    let mut bad = Mcp::start(&fixture, "../assistant");
    let _ = bad.stdin.flush();
    assert!(bad.child.wait().is_ok_and(|status| !status.success()));
    let frames = session.drain();
    assert_eq!(count(&frames, "tool/call"), 0, "{frames:?}");
}

#[test]
fn heartbeat_pauses_while_a_bridge_call_is_pending() {
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 8);
    session.start_turn_with("delegate it", "turn-1", Some(tools()));
    wait_for(&sock(&fixture), Duration::from_secs(3));
    let mut mcp = Mcp::start(&fixture, "assistant");
    mcp.call("delegate_task", json!({}));
    // Progress frames queued before the call are fine; read up to the call.
    let call = session.next_tool_call();
    let quiet_since = Instant::now();
    while quiet_since.elapsed() < Duration::from_millis(2_600) {
        if let Ok((_, frame)) = session.frames.recv_timeout(Duration::from_millis(100)) {
            panic!("heartbeat while a bridge call was pending: {frame}");
        }
    }
    session.send(tool_result(
        call["callId"].as_str().unwrap(),
        json!({"ok":true,"output":"done"}),
    ));
    assert_eq!(mcp.read(Duration::from_secs(5))["result"]["isError"], false);
    let (_, frame) = session.next(MAX_FRAME_GAP);
    assert_eq!(frame["method"], "progress", "heartbeat resumes: {frame}");
    session.drain();
}

#[test]
fn bridge_rejects_oversized_and_malformed_frames() {
    use std::os::unix::net::UnixStream;
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 3);
    session.start_turn_with("delegate it", "turn-1", Some(tools()));
    wait_for(&sock(&fixture), Duration::from_secs(3));
    let ask = |payload: &[u8]| -> Value {
        let mut stream = UnixStream::connect(sock(&fixture)).unwrap();
        stream.write_all(payload).unwrap();
        stream.write_all(b"\n").unwrap();
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap()
    };
    let rejected = |answer: Value| {
        assert_eq!(answer["ok"], false, "{answer}");
        assert!(
            answer["error"]
                .as_str()
                .unwrap()
                .contains("nothing was sent")
        );
    };
    rejected(ask(b"not json"));
    rejected(ask(&vec![b'x'; 1024 * 1024 + 10]));
    rejected(ask(
        br#"{"op":"call","tool":"create_graph","arguments":{}}"#,
    ));
    rejected(ask(
        br#"{"op":"call","tool":"delegate_task","arguments":[1]}"#,
    ));
    rejected(ask(br#"{"op":"unknown"}"#));

    // The MCP child answers bad input with a JSON-RPC error and keeps serving.
    let mut mcp = Mcp::start(&fixture, "assistant");
    writeln!(mcp.stdin, "not json").unwrap();
    assert_eq!(mcp.read(Duration::from_secs(5))["error"]["code"], -32700);
    mcp.stdin.write_all(&vec![b'x'; 1024 * 1024 + 10]).unwrap();
    mcp.stdin.write_all(b"\n").unwrap();
    assert_eq!(mcp.read(Duration::from_secs(5))["error"]["code"], -32600);
    let answer = mcp.request("tools/call", json!({"arguments": {}}));
    assert_eq!(answer["error"]["code"], -32602, "{answer}");
    let answer = mcp.request("no/such/method", json!({}));
    assert_eq!(answer["error"]["code"], -32601, "{answer}");
    assert_eq!(mcp.request("ping", json!({}))["result"], json!({}));

    // None of it reached the host.
    let frames = session.drain();
    assert_eq!(count(&frames, "tool/call"), 0, "{frames:?}");
}

#[test]
fn tool_result_for_unknown_call_is_ignored() {
    let fixture = Fixture::new();
    let mut session = Session::start(&fixture, 3);
    // No turn at all.
    session.send(tool_result("omp-turn-1-1", json!({"ok":true,"output":"x"})));
    session.start_turn_with("delegate it", "turn-1", Some(tools()));
    wait_for(&sock(&fixture), Duration::from_secs(3));
    session.send(tool_result("never-issued", json!({"ok":true,"output":"x"})));
    let mut mcp = Mcp::start(&fixture, "assistant");
    mcp.call("delegate_task", json!({}));
    let call = session.next_tool_call();
    let call_id = call["callId"].as_str().unwrap().to_owned();
    // Another turn's result for the real call id does not answer it.
    let mut wrong = tool_result(&call_id, json!({"ok":true,"output":"wrong turn"}));
    wrong["params"]["turnId"] = json!("turn-9");
    session.send(wrong);
    session.send(tool_result(&call_id, json!({"ok":true,"output":"right"})));
    // A repeat of the same result is ignored once it was delivered.
    session.send(tool_result(&call_id, json!({"ok":true,"output":"again"})));
    let answer = mcp.read(Duration::from_secs(5));
    assert_eq!(text_of(&answer), "right", "{answer}");
    let frames = session.drain();
    assert!(
        frames.iter().all(|f| f.get("id").is_none()),
        "tool/result is never answered: {frames:?}"
    );
    assert_eq!(frames.last().unwrap()["params"]["outcome"], "completed");
}
