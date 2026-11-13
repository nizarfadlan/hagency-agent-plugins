//! Runs the real adapter against a fake `hc` under the conditions the Hagency
//! host imposes: a cleared environment, a short idle timeout per frame, and
//! cancel while a turn runs. The fake keeps one terminal's state in files.
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{Receiver, RecvTimeoutError, channel},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// The host reads turn frames with a short idle timeout; leave margin.
const MAX_FRAME_GAP: Duration = Duration::from_millis(1_500);
const ACCEPT_WITHIN: Duration = Duration::from_millis(500);

/// Fake hc. `$FAKE_DIR/state` holds the terminal: absent, or
/// `live <status>` (idle | working | blocked | done | none). Every call is
/// appended to `$FAKE_DIR/calls`.
const FAKE_HC: &str = r#"#!/bin/sh
d="$FAKE_DIR"
printf '%s\n' "$*" >> "$d/calls"
printf '%s' "${HOME:-}" > "$d/home"
printf '%s' "${HC_SESSION:-}" > "$d/session"
state() { cat "$d/state" 2>/dev/null; }
terminal() {
  set -- $(state)
  if [ "$2" = none ]; then st=null; else st="{\"app\":\"omp\",\"state\":\"$2\"}"; fi
  echo "{\"address\":\"t1\",\"name\":\"assistant\",\"state\":\"$1\",\"status\":$st,\"turn\":0}"
}
case "$1" in
  get)
    if [ -z "$(state)" ]; then
      echo '{"error":{"hint":"see `hc ls --all`","kind":"not_found","message":"no terminal \"assistant\""}}' >&2
      exit 3
    fi
    terminal ;;
  run)
    printf '%s\n' "$*" > "$d/run-args"
    echo "live ${FAKE_START_STATUS:-idle}" > "$d/state"
    echo '{"address":"t1","agent":"omp","name":"assistant","state":"live","turn":0}' ;;
  send)
    printf '%s' "$3" > "$d/prompt"
    sleep "${FAKE_SLEEP:-3}"
    if [ -n "${FAKE_CHECKIN:-}" ]; then
      echo '{"started":true,"status":"working","turn":0,"wait":{"ended":false,"reason":"timeout","status":"working","terminal":"t1","turn":1}}'
    else
      echo "live done" > "$d/state"
      echo "{\"started\":true,\"status\":\"idle\",\"turn\":0,\"wait\":{\"ended\":true,\"reason\":\"status\",\"status\":\"${FAKE_END:-done}\",\"terminal\":\"t1\",\"turn\":1}}"
    fi ;;
  wait)
    sleep 1
    echo "live done" > "$d/state"
    echo '{"ended":true,"reason":"status","status":"done","terminal":"t1","turn":1}' ;;
  read)
    printf '{"complete":true,"how":"screen","text":" Update Available\\n\\n %s\\n\\n Thinking about it\\n\\n PLUGIN_OK\\n\\n\\u276f\\n \\u03c0 \\u00b7 model\\n"}\n' "$(cat "$d/prompt")" ;;
  interrupt)
    touch "$d/interrupted"
    echo '{"interrupted":true}' ;;
  *) echo "unexpected $*" >&2; exit 2 ;;
esac
"#;

struct Fixture {
    dir: PathBuf,
}

impl Fixture {
    /// `core`: whether the core's socket exists.
    fn new(core: bool) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "herdr-core-runtime-conformance-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(dir.join("config")).unwrap();
        fs::create_dir_all(dir.join("run/herdr-core")).unwrap();
        fs::create_dir_all(dir.join("workspace")).unwrap();
        if core {
            fs::write(dir.join("run/herdr-core/agents.sock"), "").unwrap();
        }
        let hc = dir.join("hc");
        fs::write(&hc, FAKE_HC).unwrap();
        fs::set_permissions(&hc, fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(
            dir.join("config/settings.json"),
            json!({"values": {"hc_bin": hc, "hc_session": "agents",
                "agent_command": "/opt/agents/omp --flag"}})
            .to_string(),
        )
        .unwrap();
        Self { dir }
    }
    fn read(&self, name: &str) -> String {
        fs::read_to_string(self.dir.join(name)).unwrap_or_default()
    }
    fn terminal(&self, status: &str) {
        fs::write(self.dir.join("state"), format!("live {status}")).unwrap();
    }
    fn command(&self, binary: &str) -> Command {
        let mut command = Command::new(binary);
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HAGENCY_PLUGIN_ID", "herdr-core-runtime")
            .env("HAGENCY_PLUGIN_CONFIG_DIR", self.dir.join("config"))
            .env("XDG_RUNTIME_DIR", self.dir.join("run"))
            .env("FAKE_DIR", &self.dir);
        command
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

impl Drop for Session {
    /// A test never leaves the adapter running behind it.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Session {
    fn start(fixture: &Fixture, knobs: &[(&str, &str)]) -> Self {
        let mut command = fixture.command(env!("CARGO_BIN_EXE_herdr-core-runtime"));
        for (key, value) in knobs {
            command.env(key, value);
        }
        let mut child = command
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
    fn start_turn(&mut self, fixture: &Fixture, body: &str) -> Instant {
        let prompt = json!({
            "agent": {"name": "assistant-2"},
            "instruction": "generic participant guidance",
            "inbox": [{"message": {"body": format!("[assistant-2](https://matrix.to/#/@a:s) {body}")}}]
        });
        let sent = self.send(
            json!({"jsonrpc":"2.0","id":"turn-start","method":"turn/start","params":{
            "dispatchId":"dispatch","turnId":"turn-1","prompt":prompt.to_string(),
            "workspace":fixture.dir.join("workspace"),
            "harnessContext":{"target":"assistant","ownership":"existing"}}}),
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
            let (at, frame) = self.next(Duration::from_secs(10));
            max_gap = max_gap.max(at - last);
            last = at;
            if frame["method"] == "turn/result" {
                return (frame["params"].clone(), max_gap);
            }
            // A response to a request sent meanwhile (`turn/cancel`).
            if frame.get("id").is_some() {
                assert_eq!(frame["result"]["accepted"], true, "{frame}");
                continue;
            }
            assert_eq!(frame["method"], "progress", "unexpected frame {frame}");
        }
    }
    fn turn(fixture: &Fixture, knobs: &[(&str, &str)]) -> Value {
        let mut session = Self::start(fixture, knobs);
        let accepted = session.start_turn(fixture, "Reply with exactly: PLUGIN_OK");
        let (result, max_gap) = session.until_result(accepted);
        assert!(
            max_gap <= MAX_FRAME_GAP,
            "silent for {max_gap:?} during the turn"
        );
        session.send(json!({"jsonrpc":"2.0","id":"bye","method":"shutdown","params":null}));
        result
    }
}

/// A gone terminal (first use, or after a reboot) is created in the dispatch
/// workspace with the configured agent, then prompted; the turn
/// waits for the agent's own done status and returns the clean answer.
#[test]
fn a_missing_terminal_is_created_in_the_workspace_and_answers() {
    let fixture = Fixture::new(true);
    let result = Session::turn(&fixture, &[]);
    assert_eq!(result["outcome"], "completed", "{result}");
    assert_eq!(result["finalText"], "Thinking about it\n\nPLUGIN_OK");
    assert!(result["usage"].is_null(), "usage stays unknown");
    let run = fixture.read("run-args");
    let workspace = fixture.dir.join("workspace");
    assert!(
        run.contains(&format!(
            "--name assistant --cwd {} --json /opt/agents/omp --flag",
            workspace.display()
        )),
        "{run}"
    );
    let prompt = fixture.read("prompt");
    assert!(
        prompt.contains("@assistant-2 Reply with exactly: PLUGIN_OK"),
        "{prompt}"
    );
    assert!(!prompt.contains("generic participant guidance"), "{prompt}");
    assert_eq!(fixture.read("session"), "agents");
    assert!(!fixture.read("home").is_empty(), "hc ran without HOME");
}

/// A live idle terminal is reused, never recreated; a send that checks in
/// before the turn ends is followed by `hc wait` until the agent is done.
#[test]
fn a_live_terminal_is_reused_and_a_check_in_keeps_waiting() {
    let fixture = Fixture::new(true);
    fixture.terminal("done");
    let result = Session::turn(&fixture, &[("FAKE_CHECKIN", "1"), ("FAKE_SLEEP", "1")]);
    assert_eq!(result["outcome"], "completed", "{result}");
    assert!(
        fixture.read("run-args").is_empty(),
        "a live terminal is not recreated"
    );
    assert!(
        fixture
            .read("calls")
            .lines()
            .any(|call| call.starts_with("wait assistant"))
    );
}

/// A busy or blocked agent is refused without typing into it, and the reason
/// names what the operator must look at; an agent that ends blocked fails.
#[test]
fn busy_blocked_and_failing_agents_are_failures_with_their_reason() {
    for (status, reason) in [
        ("working", "busy with another turn"),
        ("blocked", "waiting for input in its terminal (blocked)"),
    ] {
        let fixture = Fixture::new(true);
        fixture.terminal(status);
        let result = Session::turn(&fixture, &[]);
        assert_eq!(result["outcome"], "failed", "{status}: {result}");
        assert!(
            result["error"].as_str().unwrap().contains(reason),
            "{result}"
        );
        assert!(
            fixture.read("prompt").is_empty(),
            "{status}: nothing was typed"
        );
    }
    let fixture = Fixture::new(true);
    fixture.terminal("idle");
    let result = Session::turn(&fixture, &[("FAKE_END", "blocked"), ("FAKE_SLEEP", "1")]);
    assert_eq!(result["outcome"], "failed", "{result}");
    assert!(
        result["error"].as_str().unwrap().contains("(blocked)"),
        "{result}"
    );
}

/// Without a running core the turn fails at once and hc is never invoked, so
/// no core is ever autostarted with the plugin's bare environment.
#[test]
fn without_a_core_hc_is_never_invoked() {
    let fixture = Fixture::new(false);
    let result = Session::turn(&fixture, &[]);
    assert_eq!(result["outcome"], "failed", "{result}");
    assert!(
        result["error"]
            .as_str()
            .unwrap()
            .contains("herdr-core is not running"),
        "{result}"
    );
    assert!(fixture.read("calls").is_empty(), "hc was invoked");
}

/// Cancel stops the wait, settles the turn interrupted, and presses the
/// agent's stop key.
#[test]
fn cancel_interrupts_the_turn_and_the_agent() {
    let fixture = Fixture::new(true);
    fixture.terminal("idle");
    let mut session = Session::start(&fixture, &[("FAKE_SLEEP", "30")]);
    let accepted = session.start_turn(&fixture, "long task");
    while fixture.read("prompt").is_empty() {
        assert!(
            accepted.elapsed() < Duration::from_secs(5),
            "prompt never sent"
        );
        thread::sleep(Duration::from_millis(20));
    }
    session.send(
        json!({"jsonrpc":"2.0","id":"cancel","method":"turn/cancel","params":{
        "dispatchId":"dispatch","turnId":"turn-1"}}),
    );
    let started = Instant::now();
    let (result, _) = session.until_result(accepted);
    assert_eq!(result["outcome"], "interrupted", "{result}");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "cancel took too long"
    );
    assert!(
        fixture.dir.join("interrupted").exists(),
        "the agent was not interrupted"
    );
}

fn probe(fixture: &Fixture, input: Value) -> Value {
    let mut child = fixture
        .command(env!("CARGO_BIN_EXE_herdr-core-runtime"))
        .arg("--probe")
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

/// The probe says ready when a turn could start now: the core is up and the
/// target is free or gone (the turn creates it). It never starts anything.
#[test]
fn readiness_probe_reports_the_core_and_the_target() {
    let input = json!({"dispatchId": "dispatch", "harnessTemplate": {"target": "assistant"}});
    let down = Fixture::new(false);
    assert_eq!(probe(&down, input.clone())["ready"], false);
    assert!(
        down.read("calls").is_empty(),
        "the probe autostarted nothing"
    );

    let fixture = Fixture::new(true);
    assert_eq!(
        probe(&fixture, input.clone()),
        json!({"ready": true}),
        "gone: created by the turn"
    );
    for (status, ready) in [
        ("idle", true),
        ("done", true),
        ("none", true),
        ("working", false),
        ("blocked", false),
    ] {
        fixture.terminal(status);
        assert_eq!(probe(&fixture, input.clone())["ready"], ready, "{status}");
    }
    assert!(
        fixture.read("run-args").is_empty(),
        "the probe started no agent"
    );
    assert_eq!(
        probe(&fixture, json!({}))["ready"],
        false,
        "no target configured"
    );
}

/// A terminal a core restart left `finished` is started again with the agent
/// under the same name, never prompted as it is.
#[test]
fn a_finished_terminal_is_started_again() {
    let fixture = Fixture::new(true);
    fs::write(fixture.dir.join("state"), "finished none").unwrap();
    let result = Session::turn(&fixture, &[("FAKE_SLEEP", "1")]);
    assert_eq!(result["outcome"], "completed", "{result}");
    assert!(
        fixture.read("run-args").contains("--name assistant"),
        "the finished terminal was not started again"
    );
}
