package main

import (
	"context"
	"fmt"
	"time"

	"go.klarlabs.de/mcp"
)

// Host-configured TTLs exercise the modern registry without client task options.
func taskInputHTTP(ctx context.Context, address string) error {
	srv := mcp.NewServer(mcp.ServerInfo{Name: "task-inputs", Version: "1"})
	type input struct {
		Key string `json:"key" jsonschema:"required"`
	}
	srv.Tool("ask").TaskSupport(mcp.TaskSupportRequired).Handler(func(ctx context.Context, in input) (string, error) {
		elicitor := mcp.ElicitFromContext(ctx)
		if elicitor == nil {
			return "", fmt.Errorf("elicitation unavailable")
		}
		var result *mcp.ElicitResult
		var err error
		switch in.Key {
		case "form":
			result, err = elicitor.Elicit(ctx, &mcp.ElicitRequest{
				Mode: "form", Message: "What is your name?",
				RequestedSchema: map[string]any{"type": "object", "properties": map[string]any{"name": map[string]any{"type": "string"}}},
			})
		case "url":
			result, err = elicitor.ElicitURL(ctx, "Open the consent page", "https://example.test/consent", "consent-1")
		default:
			return "", fmt.Errorf("unknown input mode")
		}
		if err != nil {
			return "", err
		}
		switch result.Action {
		case "accept", "decline", "cancel":
		default:
			return "", fmt.Errorf("unknown action")
		}
		if in.Key == "form" && result.Action == "accept" && result.Content["name"] != "Ada" {
			return "", fmt.Errorf("expected Ada")
		}
		return in.Key + ":" + result.Action, nil
	})
	srv.Tool("sequence").TaskSupport(mcp.TaskSupportRequired).Handler(func(ctx context.Context, in input) (string, error) {
		elicitor := mcp.ElicitFromContext(ctx)
		if elicitor == nil {
			return "", fmt.Errorf("elicitation unavailable")
		}
		answer, err := elicitor.Elicit(ctx, &mcp.ElicitRequest{
			Mode: "form", Message: "Choose a name",
			RequestedSchema: map[string]any{"type": "object", "properties": map[string]any{"name": map[string]any{"type": "string"}}, "required": []string{"name"}},
		})
		if err != nil {
			return "", err
		}
		name, ok := answer.Content["name"].(string)
		if answer.Action != "accept" || !ok || (name != "Ada" && name != "Grace") {
			return "", fmt.Errorf("expected Ada or Grace")
		}
		confirmation, err := elicitor.Elicit(ctx, &mcp.ElicitRequest{
			Mode: "form", Message: "Confirm " + name,
			RequestedSchema: map[string]any{"type": "object", "properties": map[string]any{"confirm": map[string]any{"type": "boolean"}}, "required": []string{"confirm"}},
		})
		if err != nil {
			return "", err
		}
		if confirmation.Action != "accept" || confirmation.Content["confirm"] != true {
			return "", fmt.Errorf("expected confirmation")
		}
		return name, nil
	})
	for _, name := range []string{"retained", "overdue"} {
		srv.Tool(name).Handler(func(in input) (string, error) {
			ttl := int64(1500)
			task, err := srv.StartAugmentedCall(ctx, &ttl, func(runCtx context.Context) (any, bool, error) {
				if name == "overdue" {
					// Go expiry removes the entry without cancelling its executor.
					// Bound this fixture operation independently of registry eviction.
					timer := time.NewTimer(5 * time.Second)
					defer timer.Stop()
					select {
					case <-timer.C:
						return nil, false, fmt.Errorf("fixture deadline")
					case <-runCtx.Done():
						return nil, false, runCtx.Err()
					case <-ctx.Done():
						return nil, false, ctx.Err()
					}
				}
				return map[string]any{"content": []any{map[string]any{"type": "text", "text": "retained"}}}, false, nil
			})
			if err != nil {
				return "", err
			}
			return task.TaskID, nil
		})
	}
	return mcp.ServeHTTP(ctx, srv, address)
}
