package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go.klarlabs.de/mcp/protocol"
	"go.klarlabs.de/mcp/transport"
	"sync/atomic"
)

// Exercise the real pinned HTTP streaming, tagging, backpressure, and disconnect
// implementation with a deterministic handler. The separate --subscription-http
// mode uses the actual application handler for modern per-request negotiation.
func subscriptionTransport(ctx context.Context, address string) error {
	http := transport.NewHTTP(address, transport.WithStreamable())
	var opened, disconnected atomic.Int64
	http.SetDisconnectHook(func(_ string) { disconnected.Add(1) })
	handler := transport.HandlerFunc(func(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
		switch req.Method {
		case "subscriptions/listen":
			var params struct {
				URIs []string                   `json:"uris"`
				Meta map[string]json.RawMessage `json:"_meta"`
			}
			if err := json.Unmarshal(req.Params, &params); err != nil {
				return nil, err
			}
			if len(params.URIs) != 1 || params.URIs[0] != "users://Felix" || string(params.Meta["io.modelcontextprotocol/protocolVersion"]) != `"2026-07-28"` {
				return protocol.NewErrorResponse(req.ID, protocol.NewInvalidParams("Go resource filter/metadata differs")), nil
			}
			id := fmt.Sprintf("go-quoted\"slash\\%d", opened.Add(1))
			return protocol.NewResponse(req.ID, map[string]any{"subscriptionId": id}), nil
		case "fixture/publish":
			var params struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(req.Params, &params); err != nil {
				return nil, err
			}
			if err := http.NotifySubscription(params.ID, "notifications/resources/updated", map[string]any{"uri": "users://Felix"}); err != nil {
				return protocol.NewErrorResponse(req.ID, protocol.NewInternalError(err.Error())), nil
			}
			return protocol.NewResponse(req.ID, map[string]any{}), nil
		case "fixture/stats":
			return protocol.NewResponse(req.ID, map[string]any{"opened": opened.Load(), "disconnected": disconnected.Load()}), nil
		default:
			return protocol.NewErrorResponse(req.ID, protocol.NewMethodNotFound(req.Method)), nil
		}
	})
	return http.Serve(ctx, handler)
}
