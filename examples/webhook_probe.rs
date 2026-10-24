//! Emit the bounded webhook interoperability fixture to a supplied receiver.
use mcp_rs::transport::webhook::{WebhookConfig, WebhookNotifier};
use reqwest::{Url, header::HeaderValue};
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::args()
        .nth(1)
        .ok_or("usage: webhook_probe <HTTP(S) receiver>")?;
    let mut config = WebhookConfig::new(Url::parse(&endpoint)?);
    config
        .headers
        .insert("x-hook", HeaderValue::from_static("1"));
    let notifier = WebhookNotifier::from_config(config)?;
    notifier
        .send_channel_message("status", &json!({"type":"text", "text":"hi"}))
        .await?;
    notifier
        .send_notification(
            "notifications/resources/updated",
            &json!({"uri":"data://item"}),
        )
        .await?;
    notifier
        .send_notification("notifications/test", &Value::Null)
        .await?;
    Ok(())
}
