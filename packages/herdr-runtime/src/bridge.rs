//! MCP bridge: lets the agent in a Herdr pane call the host's task tools.
//!
//! The plugin process (one per dispatch) serves a Unix socket only while a
//! turn that was offered tools runs. `herdr-runtime --mcp --target <t>` is a
//! stdio MCP server that OMP starts from its own `mcp.json`; it relays
//! `tools/list` and `tools/call` over that socket. The plugin turns a call
//! into one `tool/call` frame for the host and returns the host's
//! `tool/result` to the child. The host owns every decision: the catalog, the
//! owner approval, the capability and duplicate-call checks.
//!
//! The bridge never forwards environment, and nothing here reads or names a
//! runner capability. With no active turn the child refuses and sends nothing.
use crate::{
    MAX_FRAME_BYTES, POLL, clean, emit, notification, passwd_home, setting_string, settings,
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::{self, BufRead, BufReader, Write},
    os::unix::{
        fs::{DirBuilderExt, FileTypeExt, OpenOptionsExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    process,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{Sender, channel},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// A tool result can be as large as the host's own 1 MiB frame plus framing.
const MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const MAX_CONNECTIONS: usize = 8;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MCP_PROTOCOL: &str = "2025-06-18";

const INSTRUCTIONS: &str = "Hagency task tools for this agent. They only work while Hagency has \
dispatched a turn to you; outside a turn every call is refused and nothing is sent. \
Delegate or message another agent only when your request asks you to, and name the exact \
engagement ID you were given; no tool tells you a peer's engagement ID. Every call except \
comment_task needs the owner's approval and can take minutes. A refused or expired call is \
final for that call: report the refusal and do not repeat it. read_peer_inbox and the \
conversation tools only reach conversations you belong to. Graph tools are not available.";

// ------------------------------------------------------------------- paths

/// A target becomes a file name, so it must stay inside the bridge directory.
pub fn valid_target(target: &str) -> bool {
    let mut chars = target.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphanumeric())
        && target.len() <= 64
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

fn default_dir() -> Option<PathBuf> {
    passwd_home()
        .or_else(|| env::var("HOME").ok().and_then(|home| clean(&home)))
        .map(|home| PathBuf::from(home).join(".hagency/herdr-bridge"))
}

/// The `bridge_dir` setting, else `<home>/.hagency/herdr-bridge`. Absolute only.
pub fn bridge_dir(settings: &Value) -> Option<PathBuf> {
    setting_string(settings, "bridge_dir")
        .map(PathBuf::from)
        .or_else(default_dir)
        .filter(|dir| dir.is_absolute())
}

fn socket_path(dir: &Path, target: &str) -> PathBuf {
    dir.join(format!("{target}.sock"))
}

fn catalog_path(dir: &Path, target: &str) -> PathBuf {
    dir.join(format!("{target}.catalog.json"))
}

// ------------------------------------------------------------------- lines

pub enum Line {
    Text(Vec<u8>),
    Oversized,
    Eof,
}

/// One `\n`-terminated line of at most `max` bytes. A longer line is read to
/// its end and discarded so the stream stays in step.
pub fn read_line<R: BufRead>(reader: &mut R, max: usize) -> io::Result<Line> {
    let mut line = Vec::new();
    let mut over = false;
    loop {
        let available = match reader.fill_buf() {
            Ok(available) => available,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if available.is_empty() {
            return Ok(if over {
                Line::Oversized
            } else if line.is_empty() {
                Line::Eof
            } else {
                Line::Text(line)
            });
        }
        let end = available.iter().position(|byte| *byte == b'\n');
        let take = end.unwrap_or(available.len());
        if !over {
            if line.len() + take > max {
                over = true;
                line.clear();
            } else {
                line.extend_from_slice(&available[..take]);
            }
        }
        reader.consume(end.map_or(take, |index| index + 1));
        if end.is_some() {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(if over {
                Line::Oversized
            } else {
                Line::Text(line)
            });
        }
    }
}

fn write_line(writer: &mut impl Write, value: &Value) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

fn failure(message: impl Into<String>) -> Value {
    json!({"ok": false, "error": message.into()})
}

// ------------------------------------------------------------------ plugin

pub struct Shared {
    dispatch_id: String,
    turn_id: String,
    cancel: Arc<AtomicBool>,
    closed: AtomicBool,
    deadline: Instant,
    catalog: Vec<Value>,
    names: HashSet<String>,
    pending: Mutex<HashMap<String, Sender<Value>>>,
    /// Calls waiting for the host; the turn's heartbeat pauses while > 0.
    calls: Arc<AtomicUsize>,
    next: AtomicU64,
    connections: AtomicUsize,
}

impl Shared {
    /// Routes a host `tool/result` to the call waiting for it. A result for an
    /// unknown, finished or repeated call id is ignored.
    pub fn deliver(&self, dispatch_id: &str, turn_id: &str, call_id: &str, result: Value) {
        if dispatch_id != self.dispatch_id || turn_id != self.turn_id {
            return;
        }
        let waiting = self.pending.lock().ok().and_then(|mut p| p.remove(call_id));
        if let Some(sender) = waiting {
            let _ = sender.send(result);
        }
    }
}

/// The socket and catalog of one running turn. Dropping it closes the socket.
pub struct Bridge {
    shared: Arc<Shared>,
    socket: PathBuf,
    listener: Option<JoinHandle<()>>,
}

/// What `start_turn` keeps to route `tool/result` frames.
pub type Routes = Arc<Shared>;

/// The MCP form of the host's tool descriptors. Invalid entries are dropped.
fn mcp_catalog(tools: &[Value]) -> Vec<Value> {
    tools
        .iter()
        .filter_map(|tool| {
            let name = tool.get("name").and_then(Value::as_str).and_then(clean)?;
            let schema = tool
                .get("inputSchema")
                .filter(|schema| schema.is_object())
                .cloned()
                .unwrap_or_else(|| json!({"type": "object"}));
            Some(json!({
                "name": name,
                "description": tool.get("description").and_then(Value::as_str).unwrap_or(""),
                "inputSchema": schema,
                "annotations": {
                    "readOnlyHint": tool.get("readOnly").and_then(Value::as_bool).unwrap_or(false)
                }
            }))
        })
        .collect()
}

/// Atomic, owner-only rewrite so a pane starting between turns lists tools.
fn write_catalog(path: &Path, catalog: &[Value]) -> io::Result<()> {
    let temporary = path.with_extension(format!("json.tmp{}", process::id()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(json!({"tools": catalog}).to_string().as_bytes())?;
    file.sync_all()?;
    fs::rename(&temporary, path).inspect_err(|_| {
        let _ = fs::remove_file(&temporary);
    })
}

impl Bridge {
    /// Starts the bridge for a turn. `Ok(None)` when the host offered no tools:
    /// no socket and no catalog are created then.
    pub fn start(
        dir: &Path,
        target: &str,
        (dispatch_id, turn_id): (&str, &str),
        tools: Option<&Value>,
        cancel: Arc<AtomicBool>,
        deadline: Instant,
        calls: Arc<AtomicUsize>,
    ) -> Result<Option<Self>, String> {
        let catalog = tools
            .and_then(Value::as_array)
            .map(|tools| mcp_catalog(tools))
            .unwrap_or_default();
        if catalog.is_empty() {
            return Ok(None);
        }
        if !valid_target(target) {
            return Err(format!("target {target:?} cannot name a bridge socket"));
        }
        let io = |what: &str| {
            let what = what.to_owned();
            move |error: io::Error| format!("{what}: {error}")
        };
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(io("bridge directory"))?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
            .map_err(io("bridge directory permissions"))?;
        let socket = socket_path(dir, target);
        // A socket left by a crashed run is stale: this process owns the turn.
        if let Ok(meta) = fs::symlink_metadata(&socket) {
            if !meta.file_type().is_socket() {
                return Err(format!("{} exists and is not a socket", socket.display()));
            }
            fs::remove_file(&socket).map_err(io("stale bridge socket"))?;
        }
        let listener = UnixListener::bind(&socket).map_err(io("bridge socket"))?;
        let ready = (|| {
            fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))?;
            listener.set_nonblocking(true)?;
            write_catalog(&catalog_path(dir, target), &catalog)
        })();
        if let Err(error) = ready {
            let _ = fs::remove_file(&socket);
            return Err(format!("bridge setup: {error}"));
        }
        let names = catalog
            .iter()
            .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
            .collect();
        let shared = Arc::new(Shared {
            dispatch_id: dispatch_id.to_owned(),
            turn_id: turn_id.to_owned(),
            cancel,
            closed: AtomicBool::new(false),
            deadline,
            catalog,
            names,
            pending: Mutex::new(HashMap::new()),
            calls,
            next: AtomicU64::new(0),
            connections: AtomicUsize::new(0),
        });
        let served = shared.clone();
        Ok(Some(Self {
            shared,
            socket,
            listener: Some(thread::spawn(move || serve(&listener, &served))),
        }))
    }

    pub fn routes(&self) -> Routes {
        self.shared.clone()
    }

    /// Removes the socket and ends every waiting call. The catalog cache stays:
    /// it only lets a pane that starts between turns list the tools, and a call
    /// still needs a live socket.
    pub fn close(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        let _ = fs::remove_file(&self.socket);
        if let Some(listener) = self.listener.take() {
            let _ = listener.join();
        }
        if let Ok(mut pending) = self.shared.pending.lock() {
            pending.clear();
        }
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.close();
    }
}

fn serve(listener: &UnixListener, shared: &Arc<Shared>) {
    let mut handlers: Vec<JoinHandle<()>> = Vec::new();
    while !shared.closed.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                handlers.retain(|handler| !handler.is_finished());
                let _ = stream.set_nonblocking(false);
                if shared.connections.fetch_add(1, Ordering::AcqRel) >= MAX_CONNECTIONS {
                    shared.connections.fetch_sub(1, Ordering::AcqRel);
                    let _ = write_line(&mut &stream, &failure("the bridge is busy"));
                    continue;
                }
                let shared = shared.clone();
                handlers.push(thread::spawn(move || {
                    connection(&stream, &shared);
                    shared.connections.fetch_sub(1, Ordering::AcqRel);
                }));
            }
            Err(_) => thread::sleep(POLL),
        }
    }
    for handler in handlers {
        let _ = handler.join();
    }
}

fn connection(stream: &UnixStream, shared: &Shared) {
    let _ = stream.set_read_timeout(Some(REQUEST_TIMEOUT));
    let _ = stream.set_write_timeout(Some(REQUEST_TIMEOUT));
    let reply = match read_line(&mut BufReader::new(stream), MAX_FRAME_BYTES) {
        Ok(Line::Text(bytes)) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(request) => answer(shared, &request),
            Err(_) => failure("the bridge request is not valid JSON; nothing was sent"),
        },
        Ok(Line::Oversized) => failure("the bridge request exceeds 1 MiB; nothing was sent"),
        Ok(Line::Eof) | Err(_) => return,
    };
    let _ = write_line(&mut &*stream, &reply);
}

fn answer(shared: &Shared, request: &Value) -> Value {
    match request.get("op").and_then(Value::as_str) {
        Some("list") => json!({"ok": true, "tools": shared.catalog}),
        Some("call") => {
            let Some(tool) = request.get("tool").and_then(Value::as_str) else {
                return failure("tool is required; nothing was sent");
            };
            if !shared.names.contains(tool) {
                return failure(format!(
                    "{tool} is not offered for this turn; nothing was sent"
                ));
            }
            let input = request.get("arguments").cloned().unwrap_or(json!({}));
            if !input.is_object() {
                return failure("arguments must be an object; nothing was sent");
            }
            relay(shared, tool, input)
        }
        _ => failure("unsupported bridge operation; nothing was sent"),
    }
}

/// One `tool/call` frame for the host, then wait for its `tool/result`.
fn relay(shared: &Shared, tool: &str, input: Value) -> Value {
    if shared.closed.load(Ordering::Acquire) || shared.cancel.load(Ordering::Acquire) {
        return failure("the Hagency turn is no longer active; nothing was sent");
    }
    let call_id = format!(
        "omp-{}-{}",
        shared.turn_id,
        shared.next.fetch_add(1, Ordering::AcqRel) + 1
    );
    let (sender, receiver) = channel();
    match shared.pending.lock() {
        Ok(mut pending) => pending.insert(call_id.clone(), sender),
        Err(_) => return failure("the bridge is unavailable; nothing was sent"),
    };
    shared.calls.fetch_add(1, Ordering::AcqRel);
    let sent = emit(&notification(
        "tool/call",
        json!({
            "dispatchId": shared.dispatch_id,
            "turnId": shared.turn_id,
            "callId": call_id,
            "tool": tool,
            "input": input
        }),
    ));
    let outcome = if sent.is_err() {
        Err("the Hagency host is unreachable; nothing was sent".to_owned())
    } else {
        loop {
            match receiver.recv_timeout(POLL * 4) {
                Ok(result) => break Ok(result),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    break Err("the Hagency turn ended before the call was answered".to_owned());
                }
            }
            if shared.cancel.load(Ordering::Acquire) {
                break Err(
                    "the Hagency turn was cancelled; the call may have reached the host".into(),
                );
            }
            if shared.closed.load(Ordering::Acquire) {
                break Err("the Hagency turn ended before the call was answered".into());
            }
            if Instant::now() >= shared.deadline {
                break Err(
                    "the Hagency turn reached its deadline before the call was answered".into(),
                );
            }
        }
    };
    if let Ok(mut pending) = shared.pending.lock() {
        pending.remove(&call_id);
    }
    shared.calls.fetch_sub(1, Ordering::AcqRel);
    match outcome {
        Ok(result) => json!({"ok": true, "result": result}),
        Err(message) => failure(message),
    }
}

// -------------------------------------------------------------- MCP child

struct Options {
    target: String,
    dir: PathBuf,
}

fn options(args: &[String]) -> Result<Options, String> {
    let (mut target, mut dir) = (None, None);
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--target" => target = Some(value.clone()),
            "--bridge-dir" => dir = Some(PathBuf::from(value)),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let target = target.ok_or("--target is required")?;
    if !valid_target(&target) {
        return Err(format!(
            "target {target:?} is not a valid Herdr target name"
        ));
    }
    let dir = dir
        .or_else(|| bridge_dir(&settings()))
        .filter(|dir| dir.is_absolute())
        .ok_or("no absolute bridge directory; pass --bridge-dir")?;
    Ok(Options { target, dir })
}

enum Exchange {
    NoTurn,
    Failed(String),
    Reply(Value),
}

/// One request/response line over the target's own socket.
fn exchange(options: &Options, request: &Value) -> Exchange {
    let path = socket_path(&options.dir, &options.target);
    let mut stream = match UnixStream::connect(path) {
        Ok(stream) => stream,
        Err(error) => {
            return match error.kind() {
                io::ErrorKind::NotFound
                | io::ErrorKind::ConnectionRefused
                | io::ErrorKind::PermissionDenied => Exchange::NoTurn,
                _ => Exchange::Failed(error.to_string()),
            };
        }
    };
    let _ = stream.set_write_timeout(Some(REQUEST_TIMEOUT));
    if let Err(error) = write_line(&mut stream, request) {
        return Exchange::Failed(error.to_string());
    }
    // The answer waits for the owner; the plugin ends it at turn end or cancel.
    match read_line(&mut BufReader::new(&stream), MAX_RESPONSE_BYTES) {
        Ok(Line::Text(bytes)) => serde_json::from_slice(&bytes)
            .map(Exchange::Reply)
            .unwrap_or_else(|_| Exchange::Failed("the bridge answered with invalid JSON".into())),
        Ok(Line::Oversized) => Exchange::Failed("the bridge answer exceeds the frame bound".into()),
        Ok(Line::Eof) => Exchange::Failed("the bridge closed before answering".into()),
        Err(error) => Exchange::Failed(error.to_string()),
    }
}

fn cached_tools(options: &Options) -> Vec<Value> {
    let path = catalog_path(&options.dir, &options.target);
    let Ok(file) = fs::File::open(path) else {
        return Vec::new();
    };
    let mut reader = BufReader::new(io::Read::take(file, MAX_RESPONSE_BYTES as u64));
    let mut bytes = Vec::new();
    if io::Read::read_to_end(&mut reader, &mut bytes).is_err() {
        return Vec::new();
    }
    serde_json::from_slice::<Value>(&bytes)
        .ok()
        .and_then(|document| document.get("tools").and_then(Value::as_array).cloned())
        .unwrap_or_default()
}

fn list_tools(options: &Options) -> Vec<Value> {
    match exchange(options, &json!({"op": "list"})) {
        Exchange::Reply(reply) if reply["ok"] == true => {
            reply["tools"].as_array().cloned().unwrap_or_default()
        }
        _ => cached_tools(options),
    }
}

fn tool_text(message: impl Into<String>, is_error: bool) -> Value {
    json!({"content": [{"type": "text", "text": message.into()}], "isError": is_error})
}

fn call_tool(options: &Options, params: &Value) -> Result<Value, String> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .ok_or("tools/call needs a tool name")?;
    let arguments = params.get("arguments").cloned().unwrap_or(json!({}));
    if !arguments.is_object() {
        return Err("tools/call arguments must be an object".into());
    }
    let request = json!({"op": "call", "tool": name, "arguments": arguments});
    Ok(match exchange(options, &request) {
        Exchange::NoTurn => tool_text(
            format!(
                "no active Hagency turn for target {}; nothing was sent",
                options.target
            ),
            true,
        ),
        Exchange::Failed(message) => tool_text(
            format!("the Hagency bridge failed ({message}); the call may not have been sent"),
            true,
        ),
        Exchange::Reply(reply) if reply["ok"] != true => tool_text(
            reply["error"]
                .as_str()
                .unwrap_or("the bridge refused the call"),
            true,
        ),
        Exchange::Reply(reply) => {
            let result = &reply["result"];
            if result["ok"] == true {
                let output = &result["output"];
                tool_text(
                    output
                        .as_str()
                        .map_or_else(|| output.to_string(), str::to_owned),
                    false,
                )
            } else {
                tool_text(
                    result["error"]
                        .as_str()
                        .unwrap_or("the Hagency host refused the call"),
                    true,
                )
            }
        }
    })
}

fn rpc_error(id: &Value, code: i64, message: impl Into<String>) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message.into()}})
}

fn rpc_result(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

/// The reply to one MCP message; `None` for notifications.
fn mcp_reply(options: &Options, message: &Value) -> Option<Value> {
    let id = message
        .get("id")
        .filter(|id| id.is_string() || id.is_number());
    let method = message.get("method").and_then(Value::as_str);
    let (Some(id), Some(method)) = (id, method) else {
        // A notification, or a message that is neither: nothing to answer.
        return (message.get("id").is_some()
            && method.is_none()
            && message.get("result").is_none()
            && message.get("error").is_none())
        .then(|| rpc_error(&Value::Null, -32600, "invalid request"));
    };
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    Some(match method {
        "initialize" => rpc_result(
            id,
            json!({
                "protocolVersion": params
                    .get("protocolVersion")
                    .and_then(Value::as_str)
                    .unwrap_or(MCP_PROTOCOL),
                "capabilities": {"tools": {"listChanged": false}},
                "serverInfo": {"name": "hagency", "version": env!("CARGO_PKG_VERSION")},
                "instructions": INSTRUCTIONS
            }),
        ),
        "ping" => rpc_result(id, json!({})),
        "tools/list" => rpc_result(id, json!({"tools": list_tools(options)})),
        "tools/call" => match call_tool(options, &params) {
            Ok(result) => rpc_result(id, result),
            Err(message) => rpc_error(id, -32602, message),
        },
        other => rpc_error(id, -32601, format!("unsupported MCP method: {other}")),
    })
}

/// `herdr-runtime --mcp --target <t> [--bridge-dir <d>]`: stdio MCP server.
pub fn mcp_main(args: &[String]) -> io::Result<()> {
    let options = match options(args) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("herdr-runtime --mcp: {message}");
            process::exit(2);
        }
    };
    let mut input = BufReader::new(io::stdin().lock());
    loop {
        let reply = match read_line(&mut input, MAX_FRAME_BYTES)? {
            Line::Eof => return Ok(()),
            Line::Oversized => Some(rpc_error(&Value::Null, -32600, "message exceeds 1 MiB")),
            Line::Text(bytes) if bytes.iter().all(u8::is_ascii_whitespace) => None,
            Line::Text(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                Ok(message) if message.is_object() => mcp_reply(&options, &message),
                Ok(_) => Some(rpc_error(&Value::Null, -32600, "message must be an object")),
                Err(_) => Some(rpc_error(&Value::Null, -32700, "message is not valid JSON")),
            },
        };
        if let Some(reply) = reply {
            write_line(&mut io::stdout().lock(), &reply)?;
        }
    }
}
