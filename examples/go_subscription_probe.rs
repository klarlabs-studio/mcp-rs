//! Exercise the explicit Go subscription client against the pinned reference.
use mcp_rs::{
    client::go_subscriptions::{Config, Subscription},
    model::SubscriptionFilter,
};
use serde_json::{Value, json};
use std::time::Duration;

async fn control_envelope(
    client: &reqwest::Client,
    endpoint: &str,
    method: &str,
    mut params: Value,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    params["_meta"] = json!({
        "io.modelcontextprotocol/protocolVersion":"2026-07-28",
        "io.modelcontextprotocol/clientInfo":{"name":"task-probe","version":"1"},
        "io.modelcontextprotocol/clientCapabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}},"elicitation":{"form":{}}}
    });
    let value: Value = client
        .post(endpoint)
        .header("mcp-method", method)
        .header("mcp-protocol-version", "2026-07-28")
        .json(&json!({"jsonrpc":"2.0","id":"control","method":method,"params":params}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if value["jsonrpc"] != "2.0" || value["id"] != "control" {
        return Err("invalid control response envelope".into());
    }
    Ok(value)
}

async fn control(
    client: &reqwest::Client,
    endpoint: &str,
    method: &str,
    params: Value,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let value = control_envelope(client, endpoint, method, params).await?;
    if value.get("error").is_some() {
        return Err(format!("fixture rejected control request: {value}").into());
    }
    Ok(value["result"].clone())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let mode = args
        .next()
        .ok_or("expected --negotiate, --transport, or --tasks")?;
    let endpoint = args.next().ok_or("expected endpoint")?;
    let client = reqwest::Client::new();
    let mut config = Config::new(endpoint.parse()?);
    if mode == "--tasks" {
        config.capabilities = mcp_rs::model::ClientCapabilities::builder()
            .enable_tasks()
            .enable_elicitation_with(
                mcp_rs::model::ElicitationCapability::new()
                    .with_form(mcp_rs::model::FormElicitationCapability::new()),
            )
            .build();
        return tokio::time::timeout(
            Duration::from_secs(15),
            task_probe(&client, &endpoint, config),
        )
        .await?;
    }
    let filter = SubscriptionFilter::builder()
        .resource_subscription("users://Felix")
        .build();
    let mut first = Subscription::open(client.clone(), config.clone(), filter.clone()).await?;
    if mode == "--transport" {
        let mut second = Subscription::open(client.clone(), config, filter).await?;
        if first.id() == second.id() {
            return Err("subscription IDs collided".into());
        }
        for stream in [&mut first, &mut second] {
            control(
                &client,
                &endpoint,
                "fixture/publish",
                json!({"id":stream.id()}),
            )
            .await?;
            let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await??
                .ok_or("missing event")?;
            let value = serde_json::to_value(event)?;
            if value["params"]["uri"] != "users://Felix"
                || value["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"] != stream.id()
            {
                return Err("scoped notification differs".into());
            }
        }
        // An idle next may be cancelled without closing the stream.
        if tokio::time::timeout(Duration::from_millis(50), first.next())
            .await
            .is_ok()
        {
            return Err("unexpected cross-stream notification".into());
        }
        first.close();
        drop(second);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if control(&client, &endpoint, "fixture/stats", json!({})).await?["disconnected"]
                    == 2
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await??;
        println!(
            "Pinned Go HTTP transport: generated IDs, scoped pushes, cancellation-safe idle wait, close/drop cleanup"
        );
    } else if mode == "--negotiate" {
        first.close();
        println!(
            "Pinned Go application: modern metadata/resource-filter request and generated-ID streaming acknowledgement"
        );
    } else {
        return Err("unknown mode".into());
    }
    Ok(())
}

async fn task_probe(
    client: &reqwest::Client,
    endpoint: &str,
    config: Config,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Unknown IDs are protocol errors, rather than successful empty replies.
    for method in ["tasks/get", "tasks/update", "tasks/cancel"] {
        let response = control_envelope(
            client,
            endpoint,
            method,
            json!({"taskId":"missing-task", "inputResponses":{}}),
        )
        .await?;
        // Pinned Go HTTP wraps task-not-found (-32602) as internal (-32603).
        if response["error"]["code"] != -32603
            || response["error"]["message"]
                != if method == "tasks/get" {
                    "mcp: task not found: missing-task (code: -32602)"
                } else {
                    "mcp: task not found (code: -32602)"
                }
            || response.get("result").is_some()
        {
            return Err(format!("{method} did not reject an unknown task: {response}").into());
        }
    }
    let mut ids = Vec::new();
    for key in ["complete", "cancel", "ignored", "failed", "input"] {
        let ack = control(
            client,
            endpoint,
            "tools/call",
            json!({"name":"gated","arguments":{"key":key}}),
        )
        .await?;
        ids.push(ack["taskId"].as_str().ok_or("missing task ID")?.to_owned());
    }
    let [complete_id, cancel_id, _ignored_id, failed_id, input_id] = ids.as_slice() else {
        return Err("task fixture did not create all tasks".into());
    };
    // A real application must reject task filters without the tasks extension.
    match Subscription::open_with_tasks(
        client.clone(),
        Config::new(endpoint.parse()?),
        SubscriptionFilter::default(),
        [complete_id],
    )
    .await
    {
        Err(mcp_rs::client::go_subscriptions::Error::Protocol(error))
            if error.message.contains("Missing required client capability") => {}
        _ => return Err("missing tasks capability was not rejected".into()),
    }
    let mut stream = Subscription::open_with_tasks(
        client.clone(),
        config,
        SubscriptionFilter::default(),
        [
            complete_id.as_str(),
            cancel_id.as_str(),
            failed_id.as_str(),
            input_id.as_str(),
        ],
    )
    .await?;
    // No initial hints: establish current state after opening the stream.
    for id in [complete_id, cancel_id, failed_id, input_id] {
        let snapshot = control(client, endpoint, "tasks/get", json!({"taskId":id})).await?;
        if snapshot["status"] != "working" || snapshot["taskId"] != id.as_str() {
            return Err("task did not remain gated".into());
        }
    }
    control(
        client,
        endpoint,
        "tools/call",
        json!({"name":"release","arguments":{"key":"input"}}),
    )
    .await?;
    let input_event = loop {
        let event = stream
            .next()
            .await?
            .ok_or("missing input_required task event")?;
        if !matches!(
            event,
            mcp_rs::model::ServerNotification::TaskStatusNotification(_)
        ) {
            return Err("input task event was not typed".into());
        }
        let value = serde_json::to_value(event)?;
        if value["params"]["taskId"] == input_id.as_str()
            && value["params"]["status"] == "input_required"
        {
            break value;
        }
    };
    let requests = input_event["params"]["inputRequests"]
        .as_object()
        .ok_or("missing task inputRequests")?;
    let (request_id, request) = requests.iter().next().ok_or("empty task inputRequests")?;
    if requests.len() != 1
        || request["method"] != "elicitation/create"
        || request["params"]["message"] != "What is your name?"
        || request["params"]["requestedSchema"]["properties"]["name"]["type"] != "string"
    {
        return Err("task requested unexpected input kind".into());
    }
    // Compare raw snapshots: typed notification decoding adds default form mode.
    let pending = control(client, endpoint, "tasks/get", json!({"taskId":input_id})).await?;
    if pending["status"] != "input_required"
        || pending["inputRequests"].as_object().map(|r| r.len()) != Some(1)
        || pending["inputRequests"][request_id]["params"]["message"] != "What is your name?"
    {
        return Err("missing pending task input snapshot".into());
    }
    // Unknown response keys must be ignored, leaving the real request pending.
    for responses in [json!({}), json!({"unknown-key":{"action":"cancel"}})] {
        control(
            client,
            endpoint,
            "tasks/update",
            json!({"taskId":input_id,"inputResponses":responses}),
        )
        .await?;
        let snapshot = control(client, endpoint, "tasks/get", json!({"taskId":input_id})).await?;
        if snapshot["status"] != "input_required"
            || snapshot["inputRequests"] != pending["inputRequests"]
        {
            return Err(format!("irrelevant input responses changed pending input: snapshot={snapshot}, event={input_event}").into());
        }
    }
    control(
        client,
        endpoint,
        "tasks/update",
        json!({"taskId":input_id,"inputResponses":{(request_id):{"action":"accept","content":{"name":"Ada"}}}}),
    )
    .await?;
    for key in ["ignored", "complete"] {
        control(
            client,
            endpoint,
            "tools/call",
            json!({"name":"release","arguments":{"key":key}}),
        )
        .await?;
    }
    control(
        client,
        endpoint,
        "tools/call",
        json!({"name":"release","arguments":{"key":"failed"}}),
    )
    .await?;
    let mut complete_seen = false;
    let mut input_seen = false;
    let mut failure_seen = false;
    while !complete_seen || !input_seen || !failure_seen {
        let event = stream.next().await?.ok_or("missing completed task")?;
        if !matches!(
            event,
            mcp_rs::model::ServerNotification::TaskStatusNotification(_)
        ) {
            return Err("task event was not typed".into());
        }
        let value = serde_json::to_value(event)?;
        if value["params"]["taskId"] == input_id.as_str() {
            if value["params"]["status"] == "completed" {
                if value["params"]["result"]["content"][0]["text"] != "hello Ada" {
                    return Err("input-required task completion differs".into());
                }
                input_seen = true;
            }
            continue;
        }
        if value["params"]["taskId"] == failed_id.as_str() {
            if value["params"]["status"] != "failed" {
                return Err("failed task status differs".into());
            }
            if value["params"]["error"]["message"] != "deliberate task failure"
                || value["params"]["error"]["code"] != -32603
            {
                return Err("failed task error payload differs".into());
            }
            failure_seen = true;
            continue;
        }
        if value["params"]["taskId"] != complete_id.as_str() {
            return Err("unrequested task delivered".into());
        }
        if value["params"]["status"] == "completed" {
            if value["params"]["result"]["content"][0]["text"] != "done complete" {
                return Err("completed payload differs".into());
            }
            complete_seen = true;
        }
    }
    control(
        client,
        endpoint,
        "tasks/cancel",
        json!({"taskId":cancel_id}),
    )
    .await?;
    loop {
        let event = stream.next().await?.ok_or("missing cancelled task")?;
        let value = serde_json::to_value(event)?;
        if value["params"]["taskId"] != cancel_id.as_str() {
            return Err("wrong task delivered".into());
        }
        if value["params"]["status"] == "cancelled" {
            break;
        }
    }
    stream.close();
    println!(
        "Pinned Go application HTTP tasks: task filters, capability rejection, post-open snapshots, typed completion/failure payloads, cancellation, excluded task, unknown-ID errors, empty/unknown-key updates preserve pending input, input_required → tasks/update → completed"
    );
    Ok(())
}
