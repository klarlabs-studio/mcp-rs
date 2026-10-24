package main

import (
	"context"
	"fmt"
	"go.klarlabs.de/mcp"
)

func promptResourceMRTRHTTP(ctx context.Context, address string) error {
	srv := mcp.NewServer(mcp.ServerInfo{Name: "prompt-resource-mrtr", Version: "1"}, mcp.WithResultCache(777, "public"))
	run := func(ctx context.Context, mode string) (string, error) {
		// Go only injects ElicitFromContext for tool handlers. Prompt/resource
		// hosts construct the exported facade from the request-scoped session.
		session := mcp.SessionFromContext(ctx)
		if session == nil {
			return "", fmt.Errorf("session unavailable")
		}
		elicitor := mcp.NewElicitor(session)
		var answer *mcp.ElicitResult
		var err error
		switch mode {
		case "form", "sequence":
			answer, err = elicitor.Elicit(ctx, &mcp.ElicitRequest{Mode: "form", Message: "Choose a name", RequestedSchema: map[string]any{"type": "object", "properties": map[string]any{"name": map[string]any{"type": "string"}}, "required": []string{"name"}}})
		case "url":
			answer, err = elicitor.ElicitURL(ctx, "Open consent", "https://example.test/consent", "consent-1")
		default:
			return "", fmt.Errorf("unknown mode")
		}
		if err != nil {
			return "", err
		}
		if answer.Action != "accept" && answer.Action != "decline" && answer.Action != "cancel" {
			return "", fmt.Errorf("invalid action")
		}
		if mode != "url" && answer.Action == "accept" && answer.Content["name"] != "Ada" {
			return "", fmt.Errorf("expected Ada")
		}
		if mode == "sequence" {
			if answer.Action != "accept" {
				return "", fmt.Errorf("expected accept")
			}
			confirmation, err := elicitor.Elicit(ctx, &mcp.ElicitRequest{Mode: "form", Message: "Confirm Ada", RequestedSchema: map[string]any{"type": "object", "properties": map[string]any{"confirm": map[string]any{"type": "boolean"}}, "required": []string{"confirm"}}})
			if err != nil {
				return "", err
			}
			if confirmation.Action != "accept" || confirmation.Content["confirm"] != true {
				return "", fmt.Errorf("expected confirmation")
			}
			return "sequence:Ada", nil
		}
		return mode + ":" + answer.Action, nil
	}
	srv.Resource("mrtr://{mode}").Name("Input resource").MimeType("text/plain").Handler(func(ctx context.Context, uri string, params map[string]string) (*mcp.ResourceContent, error) {
		text, err := run(ctx, params["mode"])
		if err != nil {
			return nil, err
		}
		return &mcp.ResourceContent{URI: uri, MimeType: "text/plain", Text: text}, nil
	})
	srv.Prompt("input").Argument("mode", "Input mode", true).Handler(func(ctx context.Context, args map[string]string) (*mcp.PromptResult, error) {
		text, err := run(ctx, args["mode"])
		if err != nil {
			return nil, err
		}
		return &mcp.PromptResult{Description: "Input complete", Messages: []mcp.PromptMessage{{Role: "user", Content: mcp.TextContent{Type: "text", Text: text}}}}, nil
	})
	return mcp.ServeHTTP(ctx, srv, address)
}
