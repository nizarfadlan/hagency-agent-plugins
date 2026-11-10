use serde_json::{Value, json};
use std::{
    env, fs,
    io::{self, BufRead, Write},
    path::PathBuf,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const PROTOCOL: &str = "hagency.runtime/v1";

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

fn setting_string(key: &str) -> Option<String> {
    settings()
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
        .map(str::to_owned)
}

fn setting_bool(key: &str) -> Option<bool> {
    settings().get(key).and_then(Value::as_bool)
}

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
        .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
        .ok_or_else(|| format!("{key} is required"))?;
    if value.len() > max_length {
        return Err(format!("{key} exceeds {max_length} bytes"));
    }
    Ok(value.to_owned())
}

fn herdr(args: &[String], timeout_ms: u64) -> Result<Value, String> {
    if env::var("HERDR_ENV").as_deref() != Ok("1") && setting_bool("herdr_env") != Some(true) {
        return Err("HERDR_ENV=1 or herdr_env=true is required".into());
    }
    let binary = setting_string("herdr_bin")
        .or_else(|| env::var("HERDR_BIN").ok())
        .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
        .unwrap_or_else(|| "herdr".into());
    let mut child = Command::new(binary)
        .env("HERDR_ENV", "1")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("herdr spawn failed: {error}"))?;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(1));
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err("herdr command timed out".into());
            }
            Err(error) => return Err(format!("herdr wait failed: {error}")),
        }
    }
    let output = child
        .wait_with_output()
        .map_err(|error| format!("herdr output failed: {error}"))?;
    if !output.status.success() {
        let message = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        return Err(if message.is_empty() {
            format!("herdr exited with {}", output.status)
        } else {
            message
        });
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if text.is_empty() {
        Ok(Value::Null)
    } else {
        serde_json::from_str(&text).or_else(|_| Ok(Value::String(text)))
    }
}

fn target(params: &Value) -> Result<String, String> {
    let context = params
        .get("harnessContext")
        .and_then(Value::as_object)
        .ok_or_else(|| "harnessContext is required".to_owned())?;
    text(&Value::Object(context.clone()), "target", 256)
}

fn prompt_text(prompt: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(prompt) else {
        return prompt.to_owned();
    };
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

fn turn(id: &str, params: &Value) -> Vec<Value> {
    let dispatch_id = params
        .get("dispatchId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let turn_id = params
        .get("turnId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let prompt = params
        .get("prompt")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let timeout_ms = params
        .get("timeoutMs")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .clamp(1, 120_000);
    let mut frames = vec![response(id, json!({"accepted": true}))];
    frames.push(notification(
        "progress",
        json!({
            "dispatchId": dispatch_id,
            "turnId": turn_id,
            "text": "herdr: starting supervised agent turn"
        }),
    ));

    let outcome: Result<String, String> = (|| {
        let target = target(params)?;
        if dispatch_id.is_empty() || turn_id.is_empty() || prompt.is_empty() {
            return Err("dispatchId, turnId, and prompt are required".into());
        }
        let prompt_result = herdr(
            &[
                "agent".into(),
                "prompt".into(),
                target.clone(),
                prompt_text(prompt),
                "--wait".into(),
                "--timeout".into(),
                timeout_ms.to_string(),
            ],
            timeout_ms,
        )?;
        let read_result = herdr(
            &[
                "agent".into(),
                "read".into(),
                target,
                "--source".into(),
                "recent-unwrapped".into(),
                "--lines".into(),
                "120".into(),
            ],
            timeout_ms,
        )?;
        Ok(if read_result.is_null() {
            text_from(&prompt_result)
        } else {
            text_from(&read_result)
        })
    })();

    match outcome {
        Ok(text) => {
            frames.push(notification(
                "progress",
                json!({"dispatchId": dispatch_id, "turnId": turn_id, "text": text}),
            ));
            frames.push(notification(
                "turn/result",
                json!({
                    "dispatchId": dispatch_id,
                    "turnId": turn_id,
                    "outcome": "completed",
                    "finalText": text,
                    "error": null,
                    "usage": null
                }),
            ));
        }
        Err(message) => frames.push(notification(
            "turn/result",
            json!({
                "dispatchId": dispatch_id,
                "turnId": turn_id,
                "outcome": "failed",
                "finalText": null,
                "error": message,
                "usage": null
            }),
        )),
    }
    frames
}

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

fn handle(value: Value) -> Result<(Vec<Value>, bool), String> {
    let (id, method, params) = request_parts(value)?;
    let mut shutdown = false;
    let frames = match method.as_str() {
        "initialize" => {
            if params.get("protocol").and_then(Value::as_str) != Some(PROTOCOL)
                || params
                    .get("capabilities")
                    .and_then(Value::as_array)
                    .is_none()
            {
                vec![error(&id, -32602, "initialize parameters are invalid")]
            } else {
                vec![response(
                    &id,
                    json!({
                        "protocol": PROTOCOL,
                        "ready": true,
                        "capabilities": ["turn"]
                    }),
                )]
            }
        }
        "turn/start" => turn(&id, &params),
        "turn/cancel" => {
            let result = text(&params, "dispatchId", 128)
                .and(text(&params, "turnId", 128))
                .map(|_| json!({"accepted": true}));
            match result {
                Ok(result) => vec![response(&id, result)],
                Err(message) => vec![error(&id, -32602, message)],
            }
        }
        "shutdown" => {
            shutdown = true;
            vec![response(&id, json!({"accepted": true}))]
        }
        _ => vec![error(
            &id,
            -32601,
            format!("unsupported runtime method: {method}"),
        )],
    };
    Ok((frames, shutdown))
}

fn main() -> io::Result<()> {
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
        let (frames, shutdown) = handle(value).map_err(|message| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid request: {message}"),
            )
        })?;
        for frame in frames {
            emit(&frame)?;
        }
        if shutdown {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{prompt_text, target};
    use serde_json::json;

    #[test]
    fn prompt_text_extracts_dispatch_instruction() {
        assert_eq!(
            prompt_text(r#"{"instruction":"do the work"}"#),
            "do the work"
        );
        assert_eq!(prompt_text("plain prompt"), "plain prompt");
    }

    #[test]
    fn text_from_keeps_plain_string_output_unquoted() {
        assert_eq!(super::text_from(&json!("agent output")), "agent output");
    }

    #[test]
    fn target_requires_harness_context() {
        assert!(target(&json!({})).is_err());
        assert_eq!(
            target(&json!({"harnessContext":{"target":"agent-1"}})).unwrap(),
            "agent-1"
        );
    }
}
