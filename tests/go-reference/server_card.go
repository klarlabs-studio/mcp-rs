package main

import (
	"context"
	"go.klarlabs.de/mcp"
	"go.klarlabs.de/mcp/server"
)

func serverCardHTTP(ctx context.Context, address string) error {
	srv := mcp.NewServer(mcp.ServerInfo{Name: "card-fixture", Version: "1"})
	card := mcp.NewServerCard(&server.Manifest{
		Name: "card-fixture", Version: "1", Description: "Discover research tools", Title: "Research", WebsiteURL: "https://example.test",
	}, mcp.WithServerCardRemote(mcp.Remote{Type: "streamable-http", URL: "https://example.test/mcp", SupportedProtocolVersions: []string{"2026-07-28"}}),
		mcp.WithServerCardCatalogIdentifier("urn:air:example.test:mcp:card-fixture"))
	return mcp.ServeHTTP(ctx, srv, address, mcp.WithServerCard(card))
}
