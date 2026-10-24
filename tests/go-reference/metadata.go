package main

import "go.klarlabs.de/mcp"

// Native upstream options; opt-in fixtures leave the shared default app unchanged.
func serverMetadataOptions() []mcp.Option {
	return []mcp.Option{
		mcp.WithTitle("Research 🦀"),
		mcp.WithDescription("Track papers, code, and videos"),
		mcp.WithWebsiteURL("https://example.test/research"),
		mcp.WithIcons(mcp.Icon{Src: "https://example.test/icon.svg", MimeType: "image/svg+xml", Sizes: "any", Theme: "dark"}),
		mcp.WithInstructions("Use search before reading details.\nCite the original source."),
		mcp.WithBuildInfo("abc123", "2026-10-17T10:00:00Z"),
	}
}
