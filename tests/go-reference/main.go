// Command reference implements the shared cross-language parity fixture.
package main

import (
	"context"
	"errors"
	"fmt"
	"go.klarlabs.de/mcp"
	grpctransport "go.klarlabs.de/mcp/transport/grpc"
	"os"
	"os/signal"
	"syscall"
)

type Input struct {
	Name string `json:"name" jsonschema:"required"`
}
type CountInput struct {
	Count int `json:"count" jsonschema:"required,minimum=1,maximum=5"`
}
type Output struct {
	Message string `json:"message" jsonschema:"required"`
}

func main() {
	if len(os.Args) == 3 && os.Args[1] == "--webhook" {
		if err := sendWebhookFixture(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if len(os.Args) == 2 && os.Args[1] == "--discovery-stdio" {
		if err := discoveryStdio(context.Background()); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	var metadata []mcp.Option
	if len(os.Args) == 2 && os.Args[1] == "--metadata-stdio" {
		metadata = serverMetadataOptions()
	}
	if len(os.Args) == 2 {
		switch os.Args[1] {
		case "--cache-private-stdio":
			metadata = append(metadata, mcp.WithResultCache(12_345, "private"))
		case "--cache-public-stdio":
			metadata = append(metadata, mcp.WithResultCache(0, "public"))
		}
	}
	srv := mcp.NewServer(mcp.ServerInfo{Name: "parity-fixture", Version: "1", Capabilities: mcp.Capabilities{Tools: true, Resources: true, Prompts: true}}, metadata...)
	srv.Tool("hello").Description("Say hello").Handler(func(in Input) (string, error) { return "Hello " + in.Name, nil })
	srv.Tool("count").Description("Constrained input").Handler(func(in CountInput) (int, error) { return in.Count, nil })
	srv.Tool("structured").Description("Structured output").OutputSchema(Output{}).Handler(func(in Input) (Output, error) { return Output{Message: "Hello " + in.Name}, nil })
	srv.Tool("rich").Handler(func(in Input) (mcp.StructuredResult, error) {
		return mcp.StructuredResult{Content: []mcp.Content{mcp.NewTextContent("display")}, StructuredContent: map[string]any{"name": in.Name}}, nil
	})
	srv.Tool("empty").Handler(func(in Input) (mcp.StructuredResult, error) {
		return mcp.StructuredResult{Content: []mcp.Content{}, StructuredContent: map[string]any{}}, nil
	})
	srv.Tool("failure").Handler(func(in Input) (mcp.StructuredResult, error) {
		return mcp.StructuredResult{Content: []mcp.Content{mcp.NewTextContent("Expected failure")}, IsError: true}, nil
	})
	for i := 0; i < 101; i++ {
		srv.Tool(fmt.Sprintf("page_%03d", i)).Handler(func(in Input) (string, error) { return "page", nil })
	}
	srv.Resource("about://server").Name("About").MimeType("text/plain").Handler(func(ctx context.Context, uri string, params map[string]string) (*mcp.ResourceContent, error) {
		return &mcp.ResourceContent{URI: uri, MimeType: "text/plain", Text: "About the fixture"}, nil
	})
	srv.Resource("users://{id}").Name("User").MimeType("text/plain").Handler(func(ctx context.Context, uri string, params map[string]string) (*mcp.ResourceContent, error) {
		return &mcp.ResourceContent{URI: uri, MimeType: "text/plain", Text: "User " + params["id"]}, nil
	})
	srv.Resource("blob://logo").Name("Logo").MimeType("application/octet-stream").Handler(func(ctx context.Context, uri string, params map[string]string) (*mcp.ResourceContent, error) {
		return &mcp.ResourceContent{URI: uri, MimeType: "application/octet-stream", Blob: "AAEC"}, nil
	})
	srv.Prompt("greet").Description("Greeting prompt").Argument("name", "Person", true).Handler(func(ctx context.Context, args map[string]string) (*mcp.PromptResult, error) {
		return &mcp.PromptResult{Messages: []mcp.PromptMessage{{Role: "user", Content: mcp.TextContent{Type: "text", Text: "Greet " + args["name"]}}}}, nil
	})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	if len(os.Args) == 4 && (os.Args[1] == "--websocket-tls" || os.Args[1] == "--grpc-tls") {
		config, tlsErr := fixtureTLS(true, os.Args[3])
		if tlsErr != nil {
			err = tlsErr
		} else if os.Args[1] == "--websocket-tls" {
			err = mcp.ServeWebSocket(ctx, srv, os.Args[2], mcp.WithWebSocketTLSConfig(config))
		} else {
			err = mcp.ServeGRPC(ctx, srv, os.Args[2], grpctransport.WithTLSConfig(config))
		}
	} else if len(os.Args) == 4 && (os.Args[1] == "--probe-websocket-tls" || os.Args[1] == "--probe-grpc-tls") {
		err = probe(ctx, os.Args[1], os.Args[2])
	} else if len(os.Args) == 3 {
		switch os.Args[1] {
		case "--skills-http":
			err = skillsHTTP(ctx, os.Args[2])
		case "--server-card-http":
			err = serverCardHTTP(ctx, os.Args[2])
		case "--websocket":
			err = mcp.ServeWebSocket(ctx, srv, os.Args[2])
		case "--prompt-resource-mrtr-http":
			err = promptResourceMRTRHTTP(ctx, os.Args[2])
		case "--task-input-http":
			err = taskInputHTTP(ctx, os.Args[2])
		case "--task-subscription-http":
			err = taskSubscriptionHTTP(ctx, os.Args[2])
		case "--subscription-http":
			err = mcp.ServeHTTP(ctx, srv, os.Args[2])
		case "--subscription-transport":
			err = subscriptionTransport(ctx, os.Args[2])
		case "--grpc":
			err = mcp.ServeGRPC(ctx, srv, os.Args[2])
		case "--probe-websocket", "--probe-grpc":
			err = probe(ctx, os.Args[1], os.Args[2])
		default:
			err = fmt.Errorf("unknown mode")
		}
	} else if len(os.Args) == 2 && os.Args[1] == "--visibility-stdio" {
		err = mcp.ServeStdio(ctx, srv,
			mcp.WithToolFilter(func(_ context.Context, name string) bool { return name != "page_000" }),
			mcp.WithResourceFilter(func(_ context.Context, uri, name string) bool { return uri != "users://{id}" }),
			mcp.WithPromptFilter(func(_ context.Context, name string) bool { return name != "greet" }),
		)
	} else {
		err = mcp.ServeStdio(ctx, srv)
	}
	if err != nil && !(ctx.Err() != nil && errors.Is(err, context.Canceled)) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
