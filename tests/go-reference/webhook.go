package main

import (
	"go.klarlabs.de/mcp/transport"
	"net/http"
)

func sendWebhookFixture(endpoint string) error {
	notifier := transport.NewWebhookNotifier(endpoint)
	notifier.Headers = http.Header{"X-Hook": []string{"1"}}
	if err := notifier.SendChannelMessage("status", map[string]any{"type": "text", "text": "hi"}); err != nil {
		return err
	}
	if err := notifier.SendNotification("notifications/resources/updated", map[string]any{"uri": "data://item"}); err != nil {
		return err
	}
	return notifier.SendNotification("notifications/test", nil)
}
