// Progressive discovery fixture; the pinned upstream dispatcher is unmodified.
package main

import (
	"context"
	"fmt"
	"go.klarlabs.de/mcp"
)

type DiscoveryInput struct {
	Q string `json:"q" jsonschema:"required"`
}

func discoveryStdio(ctx context.Context) error {
	srv := mcp.NewServer(mcp.ServerInfo{Name: "discovery-fixture", Version: "1", Capabilities: mcp.Capabilities{Tools: true}})
	srv.Tool("billing.create").Description("Create invoice").Group("billing").Tags("write", "finance").DeferSchema().OutputSchema(DiscoveryInput{}).Handler(func(in DiscoveryInput) (DiscoveryInput, error) { return in, nil })
	srv.Tool("billing.list").Description("List invoices").Group("billing").Tags("read", "finance").OutputSchema(DiscoveryInput{}).Handler(func(in DiscoveryInput) (DiscoveryInput, error) { return in, nil })
	srv.Tool("search").Description("Search catalog").Tags("read").Handler(func(in DiscoveryInput) (string, error) { return in.Q, nil })
	for i := 0; i < 102; i++ {
		srv.Tool(fmt.Sprintf("page_%03d", i)).Description("Catalog page").Group("catalog").Tags("read", "finance").Handler(func(in DiscoveryInput) (string, error) { return in.Q, nil })
	}
	return mcp.ServeStdio(ctx, srv, mcp.WithToolFilter(func(_ context.Context, name string) bool { return name != "page_000" }))
}
