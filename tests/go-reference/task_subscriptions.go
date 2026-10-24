package main

import (
	"context"
	"fmt"
	"sync"

	"go.klarlabs.de/mcp"
)

// Exercise the real application task manager and HTTP notification routing.
// The pinned ServeHTTP wires task pushes only when ResourceSubscribe is enabled.
func taskSubscriptionHTTP(ctx context.Context, address string) error {
	gates := map[string]chan struct{}{}
	releases := map[string]*sync.Once{}
	for _, key := range []string{"complete", "cancel", "ignored", "failed", "input"} {
		gates[key] = make(chan struct{})
		releases[key] = &sync.Once{}
	}
	srv := mcp.NewServer(mcp.ServerInfo{Name: "task-subscriptions", Version: "1", Capabilities: mcp.Capabilities{Resources: true, ResourceSubscribe: true}})
	type taskInput struct {
		Key string `json:"key" jsonschema:"required"`
	}
	srv.Tool("gated").TaskSupport(mcp.TaskSupportRequired).Handler(func(ctx context.Context, in taskInput) (string, error) {
		gate, ok := gates[in.Key]
		if !ok {
			return "", fmt.Errorf("unknown gate")
		}
		select {
		case <-gate:
			if in.Key == "failed" {
				return "", fmt.Errorf("deliberate task failure")
			}
			if in.Key == "input" {
				elicitor := mcp.ElicitFromContext(ctx)
				if elicitor == nil {
					return "", fmt.Errorf("elicitation unavailable")
				}
				result, err := elicitor.Elicit(ctx, &mcp.ElicitRequest{
					Message: "What is your name?",
					RequestedSchema: map[string]any{"type": "object", "properties": map[string]any{
						"name": map[string]any{"type": "string"},
					}},
				})
				if err != nil {
					return "", err
				}
				name, ok := result.Content["name"].(string)
				if result.Action != "accept" || !ok || name == "" {
					return "", fmt.Errorf("accepted name required")
				}
				return "hello " + name, nil
			}
			return "done " + in.Key, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	srv.Tool("release").Handler(func(in taskInput) (string, error) {
		once, ok := releases[in.Key]
		if !ok {
			return "", fmt.Errorf("unknown gate")
		}
		once.Do(func() { close(gates[in.Key]) })
		return "released", nil
	})
	return mcp.ServeHTTP(ctx, srv, address)
}
