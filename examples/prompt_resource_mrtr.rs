//! Deterministic modern prompt/resource MRTR wire fixture.
use mcp_rs::{Server, Session, model::*, transport};
use serde_json::json;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

fn run(
    mode: &str,
    state: Option<String>,
    responses: Option<InputResponses>,
) -> Result<Result<String, InputRequiredResult>, ErrorData> {
    let responses = responses.unwrap_or_default();
    let request_input = |id: &str, params| -> Result<_, ErrorData> {
        let input = serde_json::from_value(json!({"method":"elicitation/create", "params":params}))
            .map_err(|_| ErrorData::internal_error("invalid fixture input", None))?;
        Ok(Err(InputRequiredResult::new(
            Some(BTreeMap::from([(id.to_owned(), input)])),
            state.clone(),
        )))
    };
    let params = match mode {
        "form" | "sequence" => {
            json!({"mode":"form","message":"Choose a name","requestedSchema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}})
        }
        "url" => {
            json!({"mode":"url","message":"Open consent","url":"https://example.test/consent","elicitationId":"consent-1"})
        }
        _ => return Err(ErrorData::invalid_params("unknown mode", None)),
    };
    let Some(answer) = responses.get("ir-0") else {
        return request_input("ir-0", params);
    };
    let action = answer["action"]
        .as_str()
        .filter(|action| matches!(*action, "accept" | "decline" | "cancel"))
        .ok_or_else(|| ErrorData::invalid_params("invalid action", None))?;
    if mode != "url" && action == "accept" && answer["content"]["name"] != "Ada" {
        return Err(ErrorData::invalid_params("expected Ada", None));
    }
    if mode == "sequence" {
        if action != "accept" {
            return Err(ErrorData::invalid_params("expected accept", None));
        }
        let Some(confirmation) = responses.get("ir-1") else {
            return request_input(
                "ir-1",
                json!({"mode":"form","message":"Confirm Ada","requestedSchema":{"type":"object","properties":{"confirm":{"type":"boolean"}},"required":["confirm"]}}),
            );
        };
        if confirmation["action"] != "accept" || confirmation["content"]["confirm"] != true {
            return Err(ErrorData::invalid_params("expected confirmation", None));
        }
        return Ok(Ok("sequence:Ada".into()));
    }
    Ok(Ok(format!("{mode}:{action}")))
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("--http") {
        return Err("expected --http <address>".into());
    }
    let address = args.next().ok_or("missing address")?;
    let mut server =
        Server::new("prompt-resource-mrtr", "1").with_result_cache(777, CacheScope::Public);
    server
        .resource("mrtr://{mode}")
        .name("Input resource")
        .mime_type("text/plain")
        .protocol_handler(|ctx, uri, params| async move {
            let session = Session::from_context(&ctx);
            let request = session
                .resource_request()
                .ok_or_else(|| ErrorData::internal_error("missing request", None))?;
            Ok(
                match run(
                    &params["mode"],
                    request.request_state.clone(),
                    request.input_responses.clone(),
                )? {
                    Ok(text) => ReadResourceResult::new(vec![
                        ResourceContents::text(text, uri).with_mime_type("text/plain"),
                    ])
                    .into(),
                    Err(input) => input.into(),
                },
            )
        })?;
    server.prompt("input").argument("mode","Input mode",true).protocol_handler(|ctx,args| async move {
        let session = Session::from_context(&ctx);
        let request = session.prompt_request().ok_or_else(|| ErrorData::internal_error("missing request",None))?;
        Ok(match run(&args["mode"],request.request_state.clone(),request.input_responses.clone())? {
            Ok(text) => serde_json::from_value::<GetPromptResult>(json!({"resultType":"complete","description":"Input complete","messages":[{"role":"user","content":{"type":"text","text":text}}]}))
                .map_err(|_| ErrorData::internal_error("invalid fixture prompt",None))?.into(),
            Err(input) => input.into(),
        })
    })?;
    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    let signal_task = tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal.cancel();
    });
    let listener = tokio::net::TcpListener::bind(address).await?;
    let result = transport::serve_http(server, listener, shutdown).await;
    signal_task.abort();
    result?;
    Ok(())
}
