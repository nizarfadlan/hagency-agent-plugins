use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{self, BufRead, Write},
};

const MAX_FRAME_BYTES: usize = 1024 * 1024;
const PROTOCOL: &str = "hagency.harness/v1";

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

fn template_values(params: &Value) -> Result<&Value, String> {
    params
        .get("template")
        .and_then(|template| template.get("values"))
        .filter(|values| values.is_object())
        .ok_or_else(|| "template values are required".into())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PreparedAgent {
    target: String,
}

type Prepared = BTreeMap<(String, String), PreparedAgent>;

fn prepare(state: &mut Prepared, params: &Value) -> Result<Value, String> {
    let allocation_id = text(params, "allocationId", 128)?;
    let dispatch_id = text(params, "dispatchId", 128)?;
    text(params, "agentName", 256)?;
    text(params, "workspace", 1024)?;

    let values = template_values(params)?;
    let target = text(values, "reference_target", 256)?;
    let ownership = text(values, "reference_ownership", 32)?;
    if !matches!(ownership.as_str(), "existing" | "managed") {
        return Err("reference_ownership must be existing or managed".into());
    }

    let key = (allocation_id, dispatch_id);
    if state.contains_key(&key) {
        return Err("agent is already prepared".into());
    }
    state.insert(
        key,
        PreparedAgent {
            target: target.clone(),
        },
    );
    Ok(json!({"target": target, "ownership": ownership}))
}

fn release(state: &mut Prepared, params: &Value) -> Result<Value, String> {
    let allocation_id = text(params, "allocationId", 128)?;
    let dispatch_id = text(params, "dispatchId", 128)?;
    let target = text(params, "target", 256)?;
    text(params, "outcome", 64)?;

    let key = (allocation_id, dispatch_id);
    let prepared = state
        .get(&key)
        .ok_or_else(|| "release has no matching prepared agent".to_owned())?;
    if prepared.target != target {
        return Err("release target does not match the prepared target".into());
    }
    state.remove(&key);
    Ok(json!({"released": true}))
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

fn handle(value: Value, state: &mut Prepared) -> Result<(Vec<Value>, bool), String> {
    let (id, method, params) = request_parts(value)?;
    let mut shutdown = false;
    let frame = match method.as_str() {
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
                    json!({
                        "protocol": PROTOCOL,
                        "ready": true,
                        "capabilities": ["snapshot", "event", "agent/prepare", "agent/release"]
                    }),
                )
            }
        }
        "snapshot" | "event" => response(&id, json!({"accepted": true})),
        "agent/prepare" => match prepare(state, &params) {
            Ok(result) => response(&id, result),
            Err(message) => error(&id, -32602, message),
        },
        "agent/release" => match release(state, &params) {
            Ok(result) => response(&id, result),
            Err(message) => error(&id, -32602, message),
        },
        "shutdown" => {
            shutdown = true;
            response(&id, json!({"accepted": true}))
        }
        _ => error(&id, -32601, format!("unsupported harness method: {method}")),
    };
    Ok((vec![frame], shutdown))
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut state = Prepared::new();
    for line in stdin.lock().lines() {
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
        let (frames, shutdown) = handle(value, &mut state).map_err(|message| {
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
    use super::{Prepared, handle};
    use serde_json::json;

    fn prepare_request() -> serde_json::Value {
        json!({
            "jsonrpc": "2.0",
            "id": "prepare",
            "method": "agent/prepare",
            "params": {
                "allocationId": "allocation-1",
                "dispatchId": "dispatch-1",
                "agentName": "reference-agent",
                "workspace": "/workspace",
                "template": {"values": {
                    "reference_target": "agent-1",
                    "reference_ownership": "managed"
                }}
            }
        })
    }

    #[test]
    fn prepare_and_release_are_bound_to_the_same_target() {
        let mut state = Prepared::new();
        let (frames, shutdown) = handle(prepare_request(), &mut state).expect("prepare");
        assert!(!shutdown);
        assert_eq!(
            frames[0]["result"],
            json!({"target": "agent-1", "ownership": "managed"})
        );

        let mut release = prepare_request();
        release["id"] = json!("release");
        release["method"] = json!("agent/release");
        release["params"] = json!({
            "allocationId": "allocation-1",
            "dispatchId": "dispatch-1",
            "target": "agent-1",
            "outcome": "completed"
        });
        let (frames, _) = handle(release, &mut state).expect("release");
        assert_eq!(frames[0]["result"]["released"], true);
        assert!(state.is_empty());
    }

    #[test]
    fn invalid_ownership_is_rejected() {
        let mut request = prepare_request();
        request["params"]["template"]["values"]["reference_ownership"] = json!("other");
        let mut state = Prepared::new();
        let (frames, _) = handle(request, &mut state).expect("request frame");
        assert_eq!(frames[0]["error"]["code"], -32602);
        assert!(state.is_empty());
    }
}
