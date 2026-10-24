// Wire probes use the actual upstream WebSocket dependency and protobuf bindings.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	pb "go.klarlabs.de/mcp/transport/grpc/mcpv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"strings"
	"time"
)

func probe(parent context.Context, mode, target string) error {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	var exchange func(string, string, map[string]any) (map[string]any, error)
	var tlsConfig *tls.Config
	if strings.HasSuffix(mode, "-tls") {
		var err error
		tlsConfig, err = fixtureTLS(false, os.Args[3])
		if err != nil {
			return err
		}
	}
	if strings.HasPrefix(mode, "--probe-websocket") {
		dialer := *websocket.DefaultDialer
		dialer.TLSClientConfig = tlsConfig
		conn, _, err := dialer.DialContext(ctx, target, nil)
		if err != nil {
			return err
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		exchange = func(id, method string, params map[string]any) (map[string]any, error) {
			params["_meta"] = map[string]any{"io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientInfo": map[string]any{"name": "go-probe", "version": "1"}, "io.modelcontextprotocol/clientCapabilities": map[string]any{}}

			request := map[string]any{"jsonrpc": "2.0", "method": method, "params": params}
			if id != "" {
				request["id"] = id
			}
			if err := conn.WriteJSON(request); err != nil {
				return nil, err
			}
			if id == "" {
				return nil, nil
			}
			for {
				var value map[string]any
				if err := conn.ReadJSON(&value); err != nil {
					return nil, err
				}
				if value["id"] == id {
					return value, nil
				}
			}
		}
	} else {
		var creds credentials.TransportCredentials = insecure.NewCredentials()
		if tlsConfig != nil {
			creds = credentials.NewTLS(tlsConfig)
		}
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds))
		if err != nil {
			return err
		}
		defer conn.Close()
		stream, err := pb.NewMCPClient(conn).Connect(ctx)
		if err != nil {
			return err
		}
		defer stream.CloseSend()
		exchange = func(id, method string, params map[string]any) (map[string]any, error) {
			params["_meta"] = map[string]any{"io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientInfo": map[string]any{"name": "go-probe", "version": "1"}, "io.modelcontextprotocol/clientCapabilities": map[string]any{}}

			data, err := json.Marshal(params)
			if err != nil {
				return nil, err
			}
			kind := pb.MessageType_MESSAGE_TYPE_REQUEST
			if id == "" {
				kind = pb.MessageType_MESSAGE_TYPE_NOTIFICATION
			}
			if err = stream.Send(&pb.Message{Type: kind, RequestId: id, Method: method, Params: data}); err != nil {
				return nil, err
			}
			if id == "" {
				return nil, nil
			}
			for {
				message, err := stream.Recv()
				if err != nil {
					return nil, err
				}
				if message.Type != pb.MessageType_MESSAGE_TYPE_RESPONSE {
					continue
				}
				if message.RequestId != id {
					return nil, fmt.Errorf("request ID changed")
				}
				value := map[string]any{"id": message.RequestId}
				if message.Error != nil {
					value["error"] = map[string]any{"code": float64(message.Error.Code)}
				} else {
					var result any
					if err = json.Unmarshal(message.Result, &result); err != nil {
						return nil, err
					}
					value["result"] = result
				}
				return value, nil
			}
		}
	}
	init, err := exchange("discover", "server/discover", map[string]any{})
	if err != nil {
		return err
	}
	if init["error"] != nil {
		return fmt.Errorf("discovery: %v", init)
	}
	id := "quoted\"slash\\newline\n"
	result, err := exchange(id, "tools/call", map[string]any{"name": "hello", "arguments": map[string]any{"name": "Felix"}})
	if err != nil {
		return err
	}
	encoded, _ := json.Marshal(result["result"])
	var tool struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err = json.Unmarshal(encoded, &tool); err != nil {
		return err
	}
	if len(tool.Content) != 1 || tool.Content[0].Text != "Hello Felix" {
		return fmt.Errorf("tool result: %v", result)
	}
	failure, err := exchange("error", "tools/call", map[string]any{"name": "does-not-exist", "arguments": map[string]any{}})
	if err != nil {
		return err
	}
	if failure["error"] == nil {
		return fmt.Errorf("missing protocol error: %v", failure)
	}
	resource, err := exchange("resource", "resources/read", map[string]any{"uri": "users://Felix"})
	if err != nil {
		return err
	}
	if resource["error"] != nil {
		return fmt.Errorf("resource: %v", resource)
	}
	data, err := json.Marshal(resource["result"])
	if err != nil {
		return err
	}
	var contents struct {
		Contents []struct {
			Text string `json:"text"`
		} `json:"contents"`
	}
	if err = json.Unmarshal(data, &contents); err != nil {
		return err
	}
	if len(contents.Contents) != 1 || contents.Contents[0].Text != "User Felix" {
		return fmt.Errorf("resource differs: %v", resource)
	}
	fmt.Println("Go wire probe: hello, escaped ID, protocol error, resource")
	return nil
}
