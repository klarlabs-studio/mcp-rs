//! Real pinned-Go prompt/resource MRTR adapter probe.
use mcp_rs::{
    client::go_mrtr::{Client, Config},
    model::*,
};
use serde_json::json;

fn answer(input: ElicitRequestParams, action: &str) -> Result<ElicitResult, ErrorData> {
    let value = serde_json::to_value(input)
        .map_err(|_| ErrorData::internal_error("fixture encoding", None))?;
    let mut answer = json!({"action":action});
    if action == "accept" && value["mode"] != "url" {
        answer["content"] = if value["message"] == "Confirm Ada" {
            json!({"confirm":true})
        } else {
            json!({"name":"Ada"})
        };
    }
    serde_json::from_value(answer).map_err(|_| ErrorData::internal_error("fixture answer", None))
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::args().nth(1).ok_or("expected endpoint")?;
    let mut config = Config::new(endpoint.parse()?);
    config.capabilities = serde_json::from_value(json!({"elicitation":{"form":{},"url":{}}}))?;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    let client = Client::new(http, config)?;
    let mut checks = 0;
    for mode in ["form", "url", "sequence"] {
        for action in ["accept", "decline", "cancel"] {
            if mode == "sequence" && action != "accept" {
                continue;
            }
            let expected = if mode == "sequence" {
                "sequence:Ada".into()
            } else {
                format!("{mode}:{action}")
            };
            let resource = client
                .read_resource(format!("mrtr://{mode}"), |input| {
                    std::future::ready(answer(input, action))
                })
                .await?;
            if resource.ttl_ms != Some(777)
                || resource.cache_scope != Some(CacheScope::Public)
                || serde_json::to_value(resource)?["contents"][0]["text"] != expected
            {
                return Err("resource mismatch".into());
            }
            checks += 1;
            let prompt = client
                .get_prompt(
                    "input",
                    Some(serde_json::from_value(json!({"mode":mode}))?),
                    |input| std::future::ready(answer(input, action)),
                )
                .await?;
            if prompt.description.as_deref() != Some("Input complete")
                || serde_json::to_value(prompt)?["messages"][0]["content"]["text"] != expected
            {
                return Err("prompt mismatch".into());
            }
            checks += 1;
        }
    }
    println!("Rust Go-MRTR adapter: {checks} prompt/resource operations passed");
    Ok(())
}
