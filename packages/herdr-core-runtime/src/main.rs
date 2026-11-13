//! `hagency.runtime/v1` adapter for a coding agent run in herdr-core (`hc`).
//!
//! One named herdr-core terminal per agent target. A turn makes sure the
//! terminal is there (creating it in the dispatch workspace when it is gone,
//! for example after a reboot), types the request, and waits for the agent's
//! own OSC 7501 status to say the turn is done. The adapter never starts the
//! core itself: a core is the machine's service, started with the operator's
//! environment, and a missing core is reported, not autostarted.
//!
//! The protocol loop on the main thread only parses frames and answers them
//! immediately; a turn runs on a worker thread that emits a `progress`
//! heartbeat every second. `turn/cancel` stops the wait and interrupts the
//! agent.
use serde_json::{Value, json};
use std::{
    env, fs,
    io::{self, BufRead, Read, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
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
const SCREEN_LINES: &str = "200";
/// How long a newly started agent may take to report its first status.
const AGENT_READY_TIMEOUT: Duration = Duration::from_secs(90);
/// One `hc send/wait` checks in at most this often; the turn waits again.
const WAIT_CHECK_IN_SECS: u64 = 120;
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

/// The home herdr-core runs under. Prefer the explicit setting, then the
/// inherited `HOME` (the host passes its own), then the home directory of the
/// user this process runs as.
fn core_home(settings: &Value) -> Option<String> {
    setting_string(settings, "hc_home")
        .or_else(|| env::var("HOME").ok().and_then(|home| clean(&home)))
        .or_else(passwd_home)
}

fn running_uid() -> Option<String> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    Some(
        status
            .lines()
            .find_map(|line| line.strip_prefix("Uid:"))?
            .split_whitespace()
            .next()?
            .to_owned(),
    )
}

fn passwd_home() -> Option<String> {
    home_for_uid(&fs::read_to_string("/etc/passwd").ok()?, &running_uid()?)
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
        // One line: hc pastes the prompt into the agent's input box.
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

/// `hc read` returns the agent's whole screen. Keep what the agent
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
}

impl TurnContext {
    fn heartbeat(&mut self) {
        if self.last_heartbeat.elapsed() < HEARTBEAT {
            return;
        }
        self.last_heartbeat = Instant::now();
        let _ = emit(&notification(
            "progress",
            json!({
                "dispatchId": self.dispatch_id,
                "turnId": self.turn_id,
                "text": format!("herdr-core: agent working ({}s)", self.started.elapsed().as_secs())
            }),
        ));
    }
}

fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// The herdr-core session the adapter talks to (`HC_SESSION`); `default`
/// when unset, as `hc` itself uses.
fn core_session(settings: &Value) -> String {
    setting_string(settings, "hc_session").unwrap_or_else(|| "default".into())
}

/// Where the session's core listens. `hc` autostarts a core when it finds
/// none, and that core would inherit this plugin's bare environment; the
/// adapter checks the socket first and never lets that happen.
fn core_socket(settings: &Value) -> Option<PathBuf> {
    let runtime = env::var("XDG_RUNTIME_DIR")
        .ok()
        .and_then(|dir| clean(&dir))
        .map(PathBuf::from)
        .or_else(|| running_uid().map(|uid| PathBuf::from(format!("/run/user/{uid}"))))?;
    Some(
        runtime
            .join("herdr-core")
            .join(format!("{}.sock", core_session(settings))),
    )
}

fn core_running(settings: &Value) -> Result<(), String> {
    match core_socket(settings) {
        Some(socket) if socket.exists() => Ok(()),
        Some(socket) => Err(format!(
            "herdr-core is not running (no socket at {}); start its service",
            socket.display()
        )),
        None => Err("herdr-core's runtime directory is unknown".into()),
    }
}

/// The `hc` command a turn or a probe runs: the configured executable, the
/// session, and the home the core runs under.
fn hc_command(settings: &Value, args: &[String]) -> Command {
    let binary = setting_string(settings, "hc_bin")
        .or_else(|| env::var("HC_BIN").ok().and_then(|value| clean(&value)))
        .unwrap_or_else(|| "hc".into());
    let mut command = Command::new(binary);
    command
        .env("HC_SESSION", core_session(settings))
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(home) = core_home(settings) {
        command.env("HOME", home);
    }
    command
}

/// Runs one `hc` command while keeping the heartbeat, deadline and cancel
/// signal live. `hc` answers JSON; a refusal (`{"error":{...}}`) is returned
/// as that value, so the caller can tell `not_found` from a failure.
fn hc(context: &mut TurnContext, settings: &Value, args: &[String]) -> Result<Value, Stop> {
    let mut child = hc_command(settings, args)
        .spawn()
        .map_err(|error| format!("hc spawn failed: {error}"))?;
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
            return Err(Stop::Failed("hc command timed out".into()));
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                context.heartbeat();
                thread::sleep(POLL);
            }
            Err(error) => {
                stop_child(&mut child);
                return Err(Stop::Failed(format!("hc wait failed: {error}")));
            }
        }
    };
    let stdout = stdout.join().unwrap_or_default();
    let stderr = stderr.join().unwrap_or_default();
    parse_hc(status.success(), &stdout, &stderr).map_err(Stop::Failed)
}

/// One `hc --json` answer: the JSON object it printed (stdout, else stderr).
/// A failed command without a JSON refusal is an error with its stderr.
fn parse_hc(success: bool, stdout: &[u8], stderr: &[u8]) -> Result<Value, String> {
    for bytes in [stdout, stderr] {
        let text = String::from_utf8_lossy(bytes);
        if let Some(value) = text
            .lines()
            .rev()
            .find_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
            .filter(Value::is_object)
            && (success || value.get("error").is_some())
        {
            return Ok(value);
        }
    }
    let message = String::from_utf8_lossy(stderr).trim().to_owned();
    Err(if message.is_empty() {
        "hc answered no JSON".into()
    } else {
        message
    })
}

/// A refusal's `(kind, message)`, if `value` is one.
fn refusal(value: &Value) -> Option<(String, String)> {
    let error = value.get("error")?;
    Some((
        error
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("error")
            .to_owned(),
        error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("hc refused the request")
            .to_owned(),
    ))
}

/// The agent's own status (OSC 7501 `state`), when it has reported one.
fn agent_state(terminal: &Value) -> Option<&str> {
    terminal.pointer("/status/state").and_then(Value::as_str)
}

/// The agent command a missing terminal is started with, as argv.
fn agent_argv(settings: &Value) -> Vec<String> {
    setting_string(settings, "agent_command")
        .unwrap_or_else(|| "omp --continue".into())
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// Make sure the named terminal is live, creating it in the dispatch
/// workspace when it is gone (never retasking a live one). Returns once the
/// agent has reported a status other than `working`. No `--keep`: a terminal
/// lost to a core restart then reads `finished` and is started again with
/// the agent, instead of coming back as a bare shell under the same name.
fn ensure_terminal(
    context: &mut TurnContext,
    settings: &Value,
    target: &str,
    workspace: &str,
) -> Result<(), Stop> {
    let current = hc(
        context,
        settings,
        &["get".into(), target.into(), "--json".into()],
    )?;
    let live = match refusal(&current) {
        Some((kind, _)) if kind == "not_found" => false,
        Some((_, message)) => return Err(Stop::Failed(format!("hc get {target}: {message}"))),
        None => current.get("state").and_then(Value::as_str) == Some("live"),
    };
    if !live {
        if workspace.is_empty() {
            return Err(Stop::Failed(format!(
                "terminal {target} is gone and the dispatch has no workspace to start it in"
            )));
        }
        let mut args = vec![
            "run".into(),
            "--bg".into(),
            "--name".into(),
            target.into(),
            "--cwd".into(),
            workspace.into(),
            "--json".into(),
        ];
        args.extend(agent_argv(settings));
        let started = hc(context, settings, &args)?;
        if let Some((_, message)) = refusal(&started) {
            return Err(Stop::Failed(format!("hc run {target}: {message}")));
        }
    }
    let until = Instant::now() + AGENT_READY_TIMEOUT;
    loop {
        let terminal = hc(
            context,
            settings,
            &["get".into(), target.into(), "--json".into()],
        )?;
        if let Some((_, message)) = refusal(&terminal) {
            return Err(Stop::Failed(format!("hc get {target}: {message}")));
        }
        match agent_state(&terminal) {
            Some("blocked") => {
                return Err(Stop::Failed(format!(
                    "agent {target} is waiting for input in its terminal (blocked); attach with `hc attach {target}`"
                )));
            }
            Some("working") => {
                return Err(Stop::Failed(format!(
                    "agent {target} is busy with another turn"
                )));
            }
            Some(_) => return Ok(()),
            // A just-started agent has not reported yet.
            None if Instant::now() < until => thread::sleep(Duration::from_millis(500)),
            None => {
                return Err(Stop::Failed(format!(
                    "agent {target} reported no status (OSC 7501); is the agent program status enabled?"
                )));
            }
        }
    }
}

/// Seconds a single `hc` wait may take before it checks in, inside the turn.
fn wait_secs(context: &TurnContext) -> String {
    context
        .deadline
        .saturating_duration_since(Instant::now())
        .as_secs()
        .clamp(1, WAIT_CHECK_IN_SECS)
        .to_string()
}

/// The turn's end, from an `hc` wait result: done is the answer; blocked,
/// error and exit are failures with their reason; a check-in is `None`.
fn turn_end(wait: &Value, target: &str) -> Option<Result<(), String>> {
    if wait.get("ended") != Some(&Value::Bool(true)) {
        return None;
    }
    let status = wait.get("status").and_then(Value::as_str).unwrap_or("");
    Some(match (wait.get("reason").and_then(Value::as_str), status) {
        (_, "done") => Ok(()),
        (Some("exit"), _) => Err(format!("agent {target} exited during the turn")),
        (_, "blocked") => Err(format!(
            "agent {target} is waiting for input in its terminal (blocked); attach with `hc attach {target}`"
        )),
        (_, "error") => Err(format!("agent {target} reported an error")),
        (reason, status) => Err(format!(
            "agent {target} ended the turn with reason {} and status {status}",
            reason.unwrap_or("unknown")
        )),
    })
}

fn run_turn(context: &mut TurnContext, params: &Value) -> Result<String, Stop> {
    let target = target(params)?;
    let prompt = params
        .get("prompt")
        .and_then(Value::as_str)
        .filter(|prompt| !prompt.is_empty())
        .ok_or_else(|| "prompt is required".to_owned())?;
    let workspace = params
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let settings = settings();
    core_running(&settings)?;
    ensure_terminal(context, &settings, &target, &workspace)?;
    let sent = prompt_text(prompt);
    let mut wait = hc(
        context,
        &settings,
        &[
            "send".into(),
            target.clone(),
            sent.clone(),
            "--wait".into(),
            "--timeout".into(),
            wait_secs(context),
            "--json".into(),
        ],
    )?;
    if let Some((_, message)) = refusal(&wait) {
        return Err(Stop::Failed(format!("hc send {target}: {message}")));
    }
    if wait.get("started") == Some(&Value::Bool(false)) {
        return Err(Stop::Failed(format!(
            "agent {target} did not take the request"
        )));
    }
    let mut outcome = wait.get("wait").and_then(|wait| turn_end(wait, &target));
    while outcome.is_none() {
        wait = hc(
            context,
            &settings,
            &[
                "wait".into(),
                target.clone(),
                "--timeout".into(),
                wait_secs(context),
                "--json".into(),
            ],
        )?;
        if let Some((_, message)) = refusal(&wait) {
            return Err(Stop::Failed(format!("hc wait {target}: {message}")));
        }
        outcome = turn_end(&wait, &target);
    }
    outcome.expect("loop ends with an outcome")?;
    let screen = hc(
        context,
        &settings,
        &[
            "read".into(),
            target.clone(),
            "--lines".into(),
            SCREEN_LINES.into(),
            "--json".into(),
        ],
    )?;
    if let Some((_, message)) = refusal(&screen) {
        return Err(Stop::Failed(format!("hc read {target}: {message}")));
    }
    Ok(answer_from_screen(&text_from(&screen), &sent))
}

/// After a cancelled turn, press the agent's stop key (best effort, bounded,
/// independent of the cancelled context).
fn interrupt(params: &Value) {
    let Ok(target) = target(params) else {
        return;
    };
    let settings = settings();
    if core_running(&settings).is_err() {
        return;
    }
    if let Ok(mut child) = hc_command(
        &settings,
        &[
            "interrupt".into(),
            target,
            "--timeout".into(),
            "3".into(),
            "--json".into(),
        ],
    )
    .spawn()
    {
        let until = Instant::now() + Duration::from_secs(5);
        while Instant::now() < until && matches!(child.try_wait(), Ok(None)) {
            thread::sleep(POLL);
        }
        stop_child(&mut child);
    }
}

fn turn_result(dispatch_id: &str, turn_id: &str, outcome: Result<String, Stop>) -> Value {
    let (outcome, final_text, error) = match outcome {
        Ok(text) => ("completed", Value::String(text), Value::Null),
        Err(Stop::Cancelled) => ("interrupted", Value::Null, Value::Null),
        Err(Stop::Failed(message)) => ("failed", Value::Null, Value::String(message)),
    };
    notification(
        "turn/result",
        json!({
            "dispatchId": dispatch_id,
            "turnId": turn_id,
            "outcome": outcome,
            "finalText": final_text,
            "error": error,
            "usage": null
        }),
    )
}

struct Running {
    dispatch_id: String,
    turn_id: String,
    cancel: Arc<AtomicBool>,
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
    let mut context = TurnContext {
        dispatch_id: dispatch_id.clone(),
        turn_id: turn_id.clone(),
        cancel: cancel.clone(),
        deadline: now + Duration::from_millis(timeout_ms),
        started: now,
        last_heartbeat: now,
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
                "text": "herdr-core: starting supervised agent turn"
            }),
        ));
        let outcome = run_turn(&mut context, &params);
        if outcome == Err(Stop::Cancelled) {
            interrupt(&params);
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
                    json!({"protocol": PROTOCOL, "ready": true, "capabilities": ["turn"]}),
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

/// The host's readiness probe (`readiness` in the manifest): can this
/// dispatch's agent take a turn now? Read-only: it checks the core's socket
/// and asks `hc get`; it never starts a core or an agent. A missing terminal
/// is ready, because the turn creates it; a busy or blocked agent is not.
fn probe(input: &Value, settings: &Value) -> Value {
    let not_ready = |reason: String| json!({"ready": false, "reason": reason});
    let Some(target) = input
        .pointer("/harnessTemplate/target")
        .and_then(Value::as_str)
        .and_then(clean)
    else {
        return not_ready("no herdr-core target is configured for this agent".into());
    };
    if let Err(reason) = core_running(settings) {
        return not_ready(reason);
    }
    let mut child =
        match hc_command(settings, &["get".into(), target.clone(), "--json".into()]).spawn() {
            Ok(child) => child,
            Err(error) => return not_ready(format!("hc could not be started: {error}")),
        };
    let deadline = Instant::now() + PROBE_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(POLL),
            _ => {
                stop_child(&mut child);
                return not_ready("hc did not answer in time".into());
            }
        }
    };
    let mut output = [Vec::new(), Vec::new()];
    if let Some(mut pipe) = child.stdout.take() {
        let _ = pipe.read_to_end(&mut output[0]);
    }
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_end(&mut output[1]);
    }
    let terminal = match parse_hc(status.success(), &output[0], &output[1]) {
        Ok(value) => value,
        Err(reason) => return not_ready(format!("hc get {target}: {reason}")),
    };
    match refusal(&terminal) {
        Some((kind, _)) if kind == "not_found" => return json!({"ready": true}),
        Some((_, message)) => return not_ready(format!("hc get {target}: {message}")),
        None => {}
    }
    if terminal.get("state").and_then(Value::as_str) != Some("live") {
        return json!({"ready": true});
    }
    match agent_state(&terminal) {
        Some("working") => not_ready(format!("agent {target} is busy with another turn")),
        Some("blocked") => not_ready(format!(
            "agent {target} is waiting for input in its terminal (blocked)"
        )),
        _ => json!({"ready": true}),
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
    // A closed or refused session never leaves an hc command running.
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
        assert!(!sent.contains('\n'), "hc pastes a single line");
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
        let settings = json!({"hc_home": "/srv/hc"});
        assert_eq!(core_home(&settings).as_deref(), Some("/srv/hc"));
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
        let ok = turn_result("d", "t", Ok("hi".into()));
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
}
