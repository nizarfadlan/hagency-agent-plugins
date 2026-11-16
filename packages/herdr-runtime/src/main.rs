//! `hagency.runtime/v1` adapter for a Herdr-managed agent.
//!
//! The protocol loop on the main thread only parses frames and answers them
//! immediately; a turn runs on a worker thread. While Herdr works the worker
//! emits a `progress` heartbeat every second, because the host reads turn
//! frames with a short idle timeout. `turn/cancel` stops the running Herdr
//! command and settles the turn as `interrupted`.
//!
//! When the host offers task tools in `turn/start`, a Unix socket serves them
//! to the agent's MCP child for the length of the turn (see [`bridge`]).
mod bridge;

use serde_json::{Value, json};
use std::{
    env, fs,
    io::{self, BufRead, Read, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// Real coding turns need minutes; the host's own budget still bounds them.
const DEFAULT_TIMEOUT_MS: u64 = 900_000;
const MAX_TIMEOUT_MS: u64 = 1_200_000;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Well below the host's turn idle timeout.
const HEARTBEAT: Duration = Duration::from_secs(1);
const POLL: Duration = Duration::from_millis(5);
const MAX_REPLY_CHARS: usize = 16_000;
const SCREEN_LINES: &str = "120";
const PROTOCOL: &str = "hagency.runtime/v1";

// ---------------------------------------------------------------- settings

fn settings() -> Value {
    let Some(root) = env::var_os("HAGENCY_PLUGIN_CONFIG_DIR") else {
        return Value::Null;
    };
    fs::read(PathBuf::from(root).join("settings.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .map(|document| document.get("values").cloned().unwrap_or(document))
        .unwrap_or(Value::Null)
}

fn clean(value: &str) -> Option<String> {
    (!value.is_empty() && !value.chars().any(char::is_control)).then(|| value.to_owned())
}

fn setting_string(settings: &Value, key: &str) -> Option<String> {
    settings.get(key).and_then(Value::as_str).and_then(clean)
}

fn setting_bool(settings: &Value, key: &str) -> Option<bool> {
    settings.get(key).and_then(Value::as_bool)
}

/// The Hagency host starts plugins with an allowlisted environment that has
/// no `HOME`; Herdr needs one to find its server socket and config. Prefer
/// the explicit setting, then an inherited `HOME`, then the home directory of
/// the user this process runs as.
fn herdr_home(settings: &Value) -> Option<String> {
    setting_string(settings, "herdr_home")
        .or_else(|| env::var("HOME").ok().and_then(|home| clean(&home)))
        .or_else(passwd_home)
}

fn passwd_home() -> Option<String> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let uid = status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))?
        .split_whitespace()
        .next()?
        .to_owned();
    home_for_uid(&fs::read_to_string("/etc/passwd").ok()?, &uid)
}

fn home_for_uid(passwd: &str, uid: &str) -> Option<String> {
    passwd.lines().find_map(|line| {
        let fields: Vec<&str> = line.split(':').collect();
        (fields.len() >= 6 && fields[2] == uid)
            .then(|| clean(fields[5]))
            .flatten()
    })
}

// ------------------------------------------------------------------ frames

fn response(id: &str, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error(id: &str, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message.into()}
    })
}

fn notification(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "method": method, "params": params})
}

/// One frame per line; the stdout lock keeps worker and loop frames whole.
fn emit(value: &Value) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    stdout.write_all(b"\n")?;
    stdout.flush()
}

fn text(value: &Value, key: &str, max_length: usize) -> Result<String, String> {
    let value = value
        .get(key)
        .and_then(Value::as_str)
        .and_then(clean)
        .ok_or_else(|| format!("{key} is required"))?;
    if value.len() > max_length {
        return Err(format!("{key} exceeds {max_length} bytes"));
    }
    Ok(value)
}

fn target(params: &Value) -> Result<String, String> {
    let context = params
        .get("harnessContext")
        .and_then(Value::as_object)
        .ok_or_else(|| "harnessContext is required".to_owned())?;
    text(&Value::Object(context.clone()), "target", 256)
}

// ---------------------------------------------------------------- envelope

/// `[name](https://matrix.to/#/@mxid) text` becomes `@name text`.
fn strip_mentions(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some(start) = rest.find('[') {
        let after = &rest[start + 1..];
        match (after.find("]("), after.find(')')) {
            (Some(close), Some(paren))
                if close < paren && after[close + 2..].starts_with("https://matrix.to/") =>
            {
                out.push_str(&rest[..start]);
                out.push('@');
                out.push_str(&after[..close]);
                rest = &after[paren + 1..];
            }
            _ => {
                out.push_str(&rest[..=start]);
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out.trim().to_owned()
}

/// The host prompt is a JSON envelope. The request addressed to this agent is
/// `inbox[].message.body`; the top-level `instruction` is generic participant
/// guidance and is only a fallback when no inbox request exists.
fn prompt_text(prompt: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(prompt) else {
        return prompt.to_owned();
    };
    let requests: Vec<String> = value
        .get("inbox")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.pointer("/message/body").and_then(Value::as_str))
                .map(strip_mentions)
                .filter(|body| !body.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if !requests.is_empty() {
        let name = value
            .pointer("/agent/name")
            .and_then(Value::as_str)
            .unwrap_or("the agent");
        // One line: Herdr types the prompt into the agent's input box.
        return format!(
            "Matrix request for {name} (reply concisely; your answer is posted back to the chat room): {}",
            requests.join(" | ")
        );
    }
    for key in ["instruction", "prompt", "text"] {
        if let Some(text) = value.get(key).and_then(Value::as_str) {
            return text.to_owned();
        }
    }
    prompt.to_owned()
}

fn text_from(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        return text.to_owned();
    }
    for key in ["text", "output", "body"] {
        if let Some(text) = value.get(key).and_then(Value::as_str) {
            return text.to_owned();
        }
    }
    value
        .get("result")
        .filter(|result| !result.is_null())
        .map(text_from)
        .unwrap_or_else(|| value.to_string())
}

/// `herdr agent read` returns the agent's whole screen. Keep what the agent
/// printed after the last echo of the prompt, up to its input line (`❯`) or
/// status footer (`π ·`); fall back to the trimmed screen.
fn answer_from_screen(screen: &str, prompt: &str) -> String {
    let tail: String = {
        let chars: Vec<char> = prompt.chars().collect();
        chars[chars.len().saturating_sub(40)..].iter().collect()
    };
    let needle = tail.trim();
    let lines: Vec<&str> = screen.lines().collect();
    let start = if needle.is_empty() {
        None
    } else {
        lines.iter().rposition(|line| line.contains(needle))
    };
    let mut kept = Vec::new();
    for line in &lines[start.map_or(0, |index| index + 1)..] {
        let line = line.trim();
        if line.starts_with('\u{276f}') || line.starts_with("\u{3c0} \u{b7}") {
            break;
        }
        kept.push(line);
    }
    let answer = kept.join("\n").trim().to_owned();
    let answer = if answer.is_empty() {
        screen.trim().to_owned()
    } else {
        answer
    };
    answer.chars().take(MAX_REPLY_CHARS).collect()
}

// ------------------------------------------------------------------- turns

#[derive(Debug, PartialEq, Eq)]
enum Stop {
    Cancelled,
    Failed(String),
}

impl From<String> for Stop {
    fn from(message: String) -> Self {
        Self::Failed(message)
    }
}

struct TurnContext {
    dispatch_id: String,
    turn_id: String,
    cancel: Arc<AtomicBool>,
    deadline: Instant,
    started: Instant,
    last_heartbeat: Instant,
    /// Bridge calls waiting for the host. The host reads no frames while it
    /// waits for the owner, so a heartbeat per second would fill the pipe.
    bridge_calls: Arc<AtomicUsize>,
}

impl TurnContext {
    fn heartbeat(&mut self) {
        if self.last_heartbeat.elapsed() < HEARTBEAT
            || self.bridge_calls.load(Ordering::Acquire) > 0
        {
            return;
        }
        self.last_heartbeat = Instant::now();
        let _ = emit(&notification(
            "progress",
            json!({
                "dispatchId": self.dispatch_id,
                "turnId": self.turn_id,
                "text": format!("herdr: agent working ({}s)", self.started.elapsed().as_secs())
            }),
        ));
    }
}

fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// The Herdr command a turn or a probe runs: the configured executable, the
/// explicit environment opt-in, and the home Herdr finds its server under.
fn herdr_command(settings: &Value, args: &[String]) -> Result<Command, String> {
    if env::var("HERDR_ENV").as_deref() != Ok("1")
        && setting_bool(settings, "herdr_env") != Some(true)
    {
        return Err("HERDR_ENV=1 or herdr_env=true is required".into());
    }
    let binary = setting_string(settings, "herdr_bin")
        .or_else(|| env::var("HERDR_BIN").ok().and_then(|value| clean(&value)))
        .unwrap_or_else(|| "herdr".into());
    let mut command = Command::new(binary);
    command
        .env("HERDR_ENV", "1")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(home) = herdr_home(settings) {
        command.env("HOME", home);
    }
    Ok(command)
}

/// Runs one Herdr command while keeping the heartbeat, deadline and cancel
/// signal live. Output pipes are drained on threads so a large screen cannot
/// block the child.
fn herdr(context: &mut TurnContext, settings: &Value, args: &[String]) -> Result<Value, Stop> {
    let mut command = herdr_command(settings, args).map_err(Stop::Failed)?;
    let mut child = command
        .spawn()
        .map_err(|error| format!("herdr spawn failed: {error}"))?;
    let read = |pipe: Option<Box<dyn Read + Send>>| {
        thread::spawn(move || {
            let mut bytes = Vec::new();
            if let Some(mut pipe) = pipe {
                let _ = pipe.read_to_end(&mut bytes);
            }
            bytes
        })
    };
    let stdout = read(
        child
            .stdout
            .take()
            .map(|p| Box::new(p) as Box<dyn Read + Send>),
    );
    let stderr = read(
        child
            .stderr
            .take()
            .map(|p| Box::new(p) as Box<dyn Read + Send>),
    );
    let status = loop {
        if context.cancel.load(Ordering::Acquire) {
            stop_child(&mut child);
            return Err(Stop::Cancelled);
        }
        if Instant::now() >= context.deadline {
            stop_child(&mut child);
            return Err(Stop::Failed("herdr command timed out".into()));
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                context.heartbeat();
                thread::sleep(POLL);
            }
            Err(error) => {
                stop_child(&mut child);
                return Err(Stop::Failed(format!("herdr wait failed: {error}")));
            }
        }
    };
    let stdout = stdout.join().unwrap_or_default();
    let stderr = stderr.join().unwrap_or_default();
    if !status.success() {
        let message = String::from_utf8_lossy(&stderr).trim().to_owned();
        return Err(Stop::Failed(if message.is_empty() {
            format!("herdr exited with {status}")
        } else {
            message
        }));
    }
    let text = String::from_utf8_lossy(&stdout).trim().to_owned();
    Ok(if text.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&text).unwrap_or(Value::String(text))
    })
}

fn run_turn(context: &mut TurnContext, params: &Value) -> Result<Answer, Stop> {
    let target = target(params)?;
    let prompt = params
        .get("prompt")
        .and_then(Value::as_str)
        .filter(|prompt| !prompt.is_empty())
        .ok_or_else(|| "prompt is required".to_owned())?;
    let settings = settings();
    let sent = prompt_text(prompt);
    // Where the agent's own transcript ends before this turn. Best effort:
    // an agent without a transcript is answered from its screen instead.
    let before = herdr(
        context,
        &settings,
        &["agent".into(), "get".into(), target.clone()],
    )
    .ok()
    .and_then(|agent| session_mark(&agent));
    let remaining = context
        .deadline
        .saturating_duration_since(Instant::now())
        .as_millis()
        .max(1)
        .to_string();
    let prompted = herdr(
        context,
        &settings,
        &[
            "agent".into(),
            "prompt".into(),
            target.clone(),
            sent.clone(),
            "--wait".into(),
            "--timeout".into(),
            remaining,
        ],
    )?;
    if let Some(answer) = before.and_then(|mark| transcript_answer(&mark, &sent)) {
        return Ok(answer);
    }
    let screen = herdr(
        context,
        &settings,
        &[
            "agent".into(),
            "read".into(),
            target,
            "--source".into(),
            "recent-unwrapped".into(),
            "--lines".into(),
            SCREEN_LINES.into(),
        ],
    )?;
    Ok(Answer {
        text: if screen.is_null() {
            text_from(&prompted)
        } else {
            answer_from_screen(&text_from(&screen), &sent)
        },
        usage: None,
    })
}

// -------------------------------------------------------------- transcript

/// What the turn returns: the agent's answer and, when the agent's own
/// transcript reports it, the turn's token usage (never invented as zero).
#[derive(Debug, PartialEq)]
struct Answer {
    text: String,
    usage: Option<Value>,
}

/// The agent's transcript file and its length before the turn.
#[derive(Debug, PartialEq)]
struct SessionMark {
    path: PathBuf,
    offset: u64,
}

/// The most transcript a turn reads: the tail written during this turn.
const MAX_TRANSCRIPT_BYTES: u64 = 8 * 1024 * 1024;

/// `herdr agent get` names the agent's session transcript (OMP writes one
/// JSONL record per message). Only an absolute `.jsonl` path is trusted.
fn session_mark(agent: &Value) -> Option<SessionMark> {
    let session = agent
        .pointer("/result/agent/agent_session")
        .or_else(|| agent.pointer("/agent/agent_session"))?;
    if session.get("kind").and_then(Value::as_str) != Some("path") {
        return None;
    }
    let path = PathBuf::from(session.get("value").and_then(Value::as_str)?);
    if !path.is_absolute() || path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
        return None;
    }
    let offset = fs::metadata(&path).ok()?.len();
    Some(SessionMark { path, offset })
}

/// The answer this turn wrote to the transcript: after the user message that
/// carries exactly the prompt we sent, the text of the last assistant
/// message that has text. Thinking blocks are the model's own reasoning and
/// never part of the answer. Usage is summed over the turn's assistant
/// messages. None when the turn cannot be found; the screen answers then.
fn transcript_answer(mark: &SessionMark, sent: &str) -> Option<Answer> {
    let mut file = fs::File::open(&mark.path).ok()?;
    let length = file.metadata().ok()?.len();
    // A shorter file is a different transcript; read it from the start.
    let start = if length >= mark.offset {
        mark.offset
    } else {
        0
    };
    if length.saturating_sub(start) > MAX_TRANSCRIPT_BYTES {
        return None;
    }
    io::Seek::seek(&mut file, io::SeekFrom::Start(start)).ok()?;
    let mut bytes = Vec::new();
    file.take(MAX_TRANSCRIPT_BYTES)
        .read_to_end(&mut bytes)
        .ok()?;
    answer_from_transcript(&String::from_utf8_lossy(&bytes), sent)
}

fn answer_from_transcript(records: &str, sent: &str) -> Option<Answer> {
    let messages: Vec<Value> = records
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|record| record.get("type").and_then(Value::as_str) == Some("message"))
        .filter_map(|record| record.get("message").cloned())
        .collect();
    let blocks = |message: &Value, kind: &str| -> String {
        message
            .get("content")
            .and_then(Value::as_array)
            .map(|content| {
                content
                    .iter()
                    .filter(|block| block.get("type").and_then(Value::as_str) == Some(kind))
                    .filter_map(|block| block.get("text").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default()
    };
    let asked = messages.iter().rposition(|message| {
        message.get("role").and_then(Value::as_str) == Some("user")
            && blocks(message, "text").trim() == sent.trim()
    })?;
    let turn: Vec<&Value> = messages[asked + 1..]
        .iter()
        .filter(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
        .collect();
    let text = turn
        .iter()
        .rev()
        .map(|message| blocks(message, "text"))
        .find(|text| !text.trim().is_empty())?;
    let mut usage = serde_json::Map::new();
    for (from, to) in [
        ("input", "input"),
        ("output", "output"),
        ("reasoningTokens", "reasoning"),
        ("cacheRead", "cacheRead"),
        ("cacheWrite", "cacheWrite"),
    ] {
        let values: Vec<u64> = turn
            .iter()
            .filter_map(|message| message.pointer(&format!("/usage/{from}")))
            .filter_map(Value::as_u64)
            .collect();
        if !values.is_empty() {
            usage.insert(to.into(), json!(values.iter().sum::<u64>()));
        }
    }
    Some(Answer {
        text: text.trim().chars().take(MAX_REPLY_CHARS).collect(),
        usage: (!usage.is_empty()).then_some(Value::Object(usage)),
    })
}

fn turn_result(dispatch_id: &str, turn_id: &str, outcome: Result<Answer, Stop>) -> Value {
    let (outcome, final_text, error, usage) = match outcome {
        Ok(answer) => (
            "completed",
            Value::String(answer.text),
            Value::Null,
            answer.usage.unwrap_or(Value::Null),
        ),
        Err(Stop::Cancelled) => ("interrupted", Value::Null, Value::Null, Value::Null),
        Err(Stop::Failed(message)) => ("failed", Value::Null, Value::String(message), Value::Null),
    };
    notification(
        "turn/result",
        json!({
            "dispatchId": dispatch_id,
            "turnId": turn_id,
            "outcome": outcome,
            "finalText": final_text,
            "error": error,
            "usage": usage
        }),
    )
}

struct Running {
    dispatch_id: String,
    turn_id: String,
    cancel: Arc<AtomicBool>,
    bridge: Option<bridge::Routes>,
    worker: JoinHandle<()>,
}

impl Running {
    fn finished(&self) -> bool {
        self.worker.is_finished()
    }
    fn stop(self) {
        self.cancel.store(true, Ordering::Release);
        let _ = self.worker.join();
    }
}

fn start_turn(id: &str, params: &Value, running: &mut Option<Running>) -> Value {
    if running.as_ref().is_some_and(|turn| !turn.finished()) {
        return error(id, -32000, "a turn is already running");
    }
    if let Some(previous) = running.take() {
        previous.stop();
    }
    let ids = text(params, "dispatchId", 128).and_then(|d| Ok((d, text(params, "turnId", 128)?)));
    let (dispatch_id, turn_id) = match ids {
        Ok(ids) => ids,
        Err(message) => return error(id, -32602, message),
    };
    let timeout_ms = params
        .get("timeoutMs")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .clamp(1, MAX_TIMEOUT_MS);
    let cancel = Arc::new(AtomicBool::new(false));
    let now = Instant::now();
    let deadline = now + Duration::from_millis(timeout_ms);
    let bridge_calls = Arc::new(AtomicUsize::new(0));
    // Only a turn offered tools gets a socket. A bridge that cannot start does
    // not stop the turn; the agent just has no host tools, and says so.
    let (mut bridge, bridge_note) = match (target(params), bridge::bridge_dir(&settings())) {
        (Ok(target), Some(dir)) => match bridge::Bridge::start(
            &dir,
            &target,
            (&dispatch_id, &turn_id),
            params.get("tools"),
            cancel.clone(),
            deadline,
            bridge_calls.clone(),
        ) {
            Ok(bridge) => (bridge, None),
            Err(message) => (
                None,
                Some(format!("herdr: host tools unavailable: {message}")),
            ),
        },
        (_, None)
            if params
                .get("tools")
                .is_some_and(|tools| tools.as_array().is_some_and(|t| !t.is_empty())) =>
        {
            (
                None,
                Some("herdr: host tools unavailable: no bridge directory".to_owned()),
            )
        }
        _ => (None, None),
    };
    let routes = bridge.as_ref().map(bridge::Bridge::routes);
    let mut context = TurnContext {
        dispatch_id: dispatch_id.clone(),
        turn_id: turn_id.clone(),
        cancel: cancel.clone(),
        deadline,
        started: now,
        last_heartbeat: now,
        bridge_calls,
    };
    let params = params.clone();
    // The acceptance must reach the host before any provider work begins.
    let _ = emit(&response(id, json!({"accepted": true})));
    let worker = thread::spawn(move || {
        let _ = emit(&notification(
            "progress",
            json!({
                "dispatchId": context.dispatch_id,
                "turnId": context.turn_id,
                "text": "herdr: starting supervised agent turn"
            }),
        ));
        if let Some(note) = bridge_note {
            let _ = emit(&notification(
                "progress",
                json!({"dispatchId": context.dispatch_id, "turnId": context.turn_id, "text": note}),
            ));
        }
        let outcome = run_turn(&mut context, &params);
        // The socket is gone before the host sees the turn end.
        if let Some(bridge) = bridge.as_mut() {
            bridge.close();
        }
        let _ = emit(&turn_result(
            &context.dispatch_id,
            &context.turn_id,
            outcome,
        ));
    });
    *running = Some(Running {
        dispatch_id,
        turn_id,
        cancel,
        bridge: routes,
        worker,
    });
    Value::Null
}

// ---------------------------------------------------------------- protocol

fn request_parts(value: Value) -> Result<(String, String, Value), String> {
    let object = value
        .as_object()
        .ok_or_else(|| "frame must be a JSON object".to_owned())?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err("jsonrpc must be 2.0".into());
    }
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty() && id.len() <= 128 && !id.chars().any(char::is_control))
        .ok_or_else(|| "request id must be a non-empty string of at most 128 bytes".to_owned())?
        .to_owned();
    let method = object
        .get("method")
        .and_then(Value::as_str)
        .filter(|method| !method.is_empty())
        .ok_or_else(|| "request method is required".to_owned())?
        .to_owned();
    Ok((
        id,
        method,
        object.get("params").cloned().unwrap_or(Value::Null),
    ))
}

/// Handles one host request. Returns the immediate response (or `Null` when
/// it was already emitted) and whether the session ends.
fn handle(value: Value, running: &mut Option<Running>) -> Result<(Value, bool), String> {
    let (id, method, params) = request_parts(value)?;
    let reply = match method.as_str() {
        "initialize" => {
            if params.get("protocol").and_then(Value::as_str) != Some(PROTOCOL)
                || params
                    .get("capabilities")
                    .and_then(Value::as_array)
                    .is_none()
            {
                error(&id, -32602, "initialize parameters are invalid")
            } else {
                response(
                    &id,
                    json!({"protocol": PROTOCOL, "ready": true, "capabilities": ["turn", "tool"]}),
                )
            }
        }
        "turn/start" => start_turn(&id, &params, running),
        "turn/cancel" => {
            match text(&params, "dispatchId", 128)
                .and_then(|d| Ok((d, text(&params, "turnId", 128)?)))
            {
                Ok((dispatch_id, turn_id)) => {
                    let matching = running
                        .as_ref()
                        .filter(|turn| turn.dispatch_id == dispatch_id && turn.turn_id == turn_id);
                    if let Some(turn) = matching {
                        turn.cancel.store(true, Ordering::Release);
                    }
                    response(&id, json!({"accepted": true}))
                }
                Err(message) => error(&id, -32602, message),
            }
        }
        // The host's answer to a bridge call. It needs no reply; a result for
        // an unknown, finished or repeated call is ignored.
        "tool/result" => {
            let ids = text(&params, "dispatchId", 128).and_then(|d| {
                Ok((
                    d,
                    text(&params, "turnId", 128)?,
                    text(&params, "callId", 256)?,
                ))
            });
            if let (Ok((dispatch_id, turn_id, call_id)), Some(result), Some(routes)) = (
                ids,
                params.get("result").filter(|result| result.is_object()),
                running.as_ref().and_then(|turn| turn.bridge.as_ref()),
            ) {
                routes.deliver(&dispatch_id, &turn_id, &call_id, result.clone());
            }
            return Ok((Value::Null, false));
        }
        "shutdown" => {
            if let Some(turn) = running.take() {
                turn.stop();
            }
            return Ok((response(&id, json!({"accepted": true})), true));
        }
        _ => error(&id, -32601, format!("unsupported runtime method: {method}")),
    };
    Ok((reply, false))
}

// ---------------------------------------------------------------- readiness

/// The probe's own bound: well inside the host's readiness deadline.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// The host's readiness probe (`readiness` in the manifest): is the agent this
/// dispatch would prompt there and ready for input? Read-only: it only asks
/// `herdr agent get`. It answers one JSON object and never starts anything, so
/// a not-ready answer lets the host requeue the dispatch as never started.
fn probe(input: &Value, settings: &Value) -> Value {
    let not_ready = |reason: String| json!({"ready": false, "reason": reason});
    let Some(target) = input
        .pointer("/harnessTemplate/target")
        .and_then(Value::as_str)
        .and_then(clean)
    else {
        return not_ready("no Herdr target is configured for this agent".into());
    };
    let args = ["agent".to_owned(), "get".to_owned(), target.clone()];
    let mut command = match herdr_command(settings, &args) {
        Ok(command) => command,
        Err(reason) => return not_ready(reason),
    };
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => return not_ready(format!("herdr could not be started: {error}")),
    };
    let deadline = Instant::now() + PROBE_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(POLL),
            _ => {
                stop_child(&mut child);
                return not_ready("herdr did not answer in time".into());
            }
        }
    };
    let mut output = String::new();
    for pipe in [
        child.stdout.take().map(|p| Box::new(p) as Box<dyn Read>),
        child.stderr.take().map(|p| Box::new(p) as Box<dyn Read>),
    ]
    .into_iter()
    .flatten()
    {
        let _ = pipe
            .take(MAX_FRAME_BYTES as u64)
            .read_to_string(&mut output);
        output.push('\n');
    }
    let answers: Vec<Value> = output
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
        .collect();
    if let Some(message) = answers
        .iter()
        .find_map(|answer| answer.pointer("/error/message").and_then(Value::as_str))
    {
        return not_ready(format!("Herdr: {message}"));
    }
    let agent = answers
        .iter()
        .find_map(|answer| answer.pointer("/result/agent"))
        .filter(|agent| agent.is_object());
    match agent {
        _ if !status.success() => {
            not_ready(format!("herdr agent get {target} exited with {status}"))
        }
        None => not_ready(format!("Herdr reports no agent {target}")),
        Some(agent) if agent.get("interactive_ready") != Some(&Value::Bool(true)) => {
            not_ready(format!("Herdr agent {target} is not ready for input"))
        }
        Some(_) => json!({"ready": true}),
    }
}

fn probe_main() -> io::Result<()> {
    let mut input = String::new();
    io::stdin()
        .take(MAX_FRAME_BYTES as u64)
        .read_to_string(&mut input)?;
    let input = serde_json::from_str(&input).unwrap_or(Value::Null);
    emit(&probe(&input, &settings()))
}

fn main() -> io::Result<()> {
    if env::args().nth(1).as_deref() == Some("--probe") {
        return probe_main();
    }
    if env::args().nth(1).as_deref() == Some("--mcp") {
        let args: Vec<String> = env::args().skip(2).collect();
        return bridge::mcp_main(&args);
    }
    let mut running: Option<Running> = None;
    let result = (|| {
        for line in io::stdin().lock().lines() {
            let line = line?;
            if line.len() > MAX_FRAME_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "input frame exceeds 1 MiB",
                ));
            }
            let value = serde_json::from_str(&line).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid JSON frame: {error}"),
                )
            })?;
            let (reply, shutdown) = handle(value, &mut running).map_err(|message| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid request: {message}"),
                )
            })?;
            if !reply.is_null() {
                emit(&reply)?;
            }
            if shutdown {
                break;
            }
        }
        Ok(())
    })();
    // A closed or refused session never leaves a Herdr command running.
    if let Some(turn) = running.take() {
        turn.stop();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_uses_the_inbox_request_not_the_generic_instruction() {
        let envelope = json!({
            "agent": {"name": "assistant-2"},
            "instruction": "generic participant guidance",
            "inbox": [{"message": {"body": "[assistant-2](https://matrix.to/#/@a:s) list the files"}}]
        });
        let sent = prompt_text(&envelope.to_string());
        assert!(sent.contains("@assistant-2 list the files"), "{sent}");
        assert!(sent.contains("assistant-2"));
        assert!(!sent.contains("generic participant guidance"));
        assert!(!sent.contains('\n'), "Herdr types a single line");
    }

    #[test]
    fn prompt_falls_back_to_instruction_and_plain_text() {
        assert_eq!(
            prompt_text(r#"{"instruction":"do the work"}"#),
            "do the work"
        );
        assert_eq!(prompt_text(r#"{"inbox":[],"instruction":"x"}"#), "x");
        assert_eq!(prompt_text("plain prompt"), "plain prompt");
    }

    #[test]
    fn mentions_keep_names_and_other_links() {
        assert_eq!(
            strip_mentions("[bob](https://matrix.to/#/@b:s) hi"),
            "@bob hi"
        );
        assert_eq!(
            strip_mentions("see [docs](https://example.test) now"),
            "see [docs](https://example.test) now"
        );
        assert_eq!(strip_mentions("[unclosed"), "[unclosed");
    }

    #[test]
    fn screen_answer_drops_banner_echo_and_footer() {
        let prompt = "Matrix request for a (reply concisely; your answer is posted back to the chat room): say OK";
        let screen = format!(
            "  ████ banner ████\n Update available\n\n {prompt}\n\n Thinking line\n\n OK\n\n❯\n π · ⬢ model · 📁 workspace"
        );
        assert_eq!(answer_from_screen(&screen, prompt), "Thinking line\n\nOK");
    }

    #[test]
    fn screen_answer_falls_back_to_trimmed_screen_and_is_bounded() {
        assert_eq!(
            answer_from_screen("  just output  ", "absent prompt"),
            "just output"
        );
        let long = "x".repeat(MAX_REPLY_CHARS + 50);
        assert_eq!(
            answer_from_screen(&long, "").chars().count(),
            MAX_REPLY_CHARS
        );
    }

    #[test]
    fn home_comes_from_passwd_for_the_running_uid() {
        let passwd =
            "root:x:0:0:root:/root:/bin/sh\nagent:x:1013:1013::/home/agent:/bin/sh\nbroken\n";
        assert_eq!(home_for_uid(passwd, "1013").as_deref(), Some("/home/agent"));
        assert_eq!(home_for_uid(passwd, "42"), None);
    }

    #[test]
    fn home_setting_wins() {
        let settings = json!({"herdr_home": "/srv/herdr"});
        assert_eq!(herdr_home(&settings).as_deref(), Some("/srv/herdr"));
    }

    #[test]
    fn text_from_keeps_plain_string_output_unquoted() {
        assert_eq!(text_from(&json!("agent output")), "agent output");
    }

    #[test]
    fn target_requires_harness_context() {
        assert!(target(&json!({})).is_err());
        assert_eq!(
            target(&json!({"harnessContext":{"target":"agent-1"}})).unwrap(),
            "agent-1"
        );
    }

    #[test]
    fn turn_result_maps_outcomes() {
        let ok = turn_result(
            "d",
            "t",
            Ok(Answer {
                text: "hi".into(),
                usage: None,
            }),
        );
        assert_eq!(ok["params"]["outcome"], "completed");
        assert_eq!(ok["params"]["finalText"], "hi");
        assert!(
            ok["params"]["usage"].is_null(),
            "usage is unknown, never zero"
        );
        let cancelled = turn_result("d", "t", Err(Stop::Cancelled));
        assert_eq!(cancelled["params"]["outcome"], "interrupted");
        let failed = turn_result("d", "t", Err(Stop::Failed("boom".into())));
        assert_eq!(failed["params"]["outcome"], "failed");
        assert_eq!(failed["params"]["error"], "boom");
    }

    fn record(role: &str, content: Value, usage: Value) -> String {
        json!({"type": "message", "message": {"role": role, "content": content, "usage": usage}})
            .to_string()
    }

    #[test]
    fn transcript_answer_is_the_text_after_our_prompt_without_thinking() {
        let sent = "Matrix request for a (reply concisely; ...): resend";
        let records = [
            record("user", json!([{"type":"text","text":"older prompt"}]), Value::Null),
            record("assistant", json!([{"type":"text","text":"older answer"}]), json!({"input": 9})),
            json!({"type":"model_change"}).to_string(),
            record("user", json!([{"type":"text","text":sent}]), Value::Null),
            record("assistant", json!([{"type":"thinking","thinking":"Same request again."},{"type":"toolCall"}]), json!({"input": 10, "output": 2})),
            record("assistant", json!([{"type":"thinking","thinking":"Keep it concise."},{"type":"text","text":"\n\n**Summary**\n\n1. done"}]),
                json!({"input": 5, "output": 3, "reasoningTokens": 1, "cacheRead": 7, "cacheWrite": 0})),
            "not json".to_owned(),
        ]
        .join("\n");
        let answer = answer_from_transcript(&records, sent).unwrap();
        assert_eq!(answer.text, "**Summary**\n\n1. done");
        assert!(!answer.text.contains("Same request"));
        assert_eq!(
            answer.usage,
            Some(
                json!({"input": 15, "output": 5, "reasoning": 1, "cacheRead": 7, "cacheWrite": 0})
            )
        );
    }

    #[test]
    fn transcript_without_our_prompt_or_text_falls_back_to_the_screen() {
        let only_other = record("user", json!([{"type":"text","text":"other"}]), Value::Null);
        assert_eq!(answer_from_transcript(&only_other, "ours"), None);
        let thinking_only = [
            record("user", json!([{"type":"text","text":"ours"}]), Value::Null),
            record(
                "assistant",
                json!([{"type":"thinking","thinking":"hmm"}]),
                Value::Null,
            ),
        ]
        .join("\n");
        assert_eq!(answer_from_transcript(&thinking_only, "ours"), None);
    }

    #[test]
    fn session_mark_trusts_only_an_absolute_jsonl_path() {
        let dir = std::env::temp_dir().join(format!("herdr-runtime-mark-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("s.jsonl");
        fs::write(&file, "abc").unwrap();
        let agent = |value: &str, kind: &str| json!({"result":{"agent":{"agent_session":{"kind":kind,"value":value}}}});
        assert_eq!(
            session_mark(&agent(file.to_str().unwrap(), "path")),
            Some(SessionMark {
                path: file.clone(),
                offset: 3
            })
        );
        assert_eq!(session_mark(&agent("relative/s.jsonl", "path")), None);
        assert_eq!(
            session_mark(&agent(dir.join("s.txt").to_str().unwrap(), "path")),
            None
        );
        assert_eq!(session_mark(&agent(file.to_str().unwrap(), "id")), None);
        fs::remove_dir_all(&dir).unwrap();
    }
}
