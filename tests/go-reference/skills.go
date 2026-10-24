package main

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"

	"go.klarlabs.de/mcp"
)

func skillsHTTP(ctx context.Context, address string) error {
	root, err := os.MkdirTemp("", "mcp-skills-fixture-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)

	git := filepath.Join(root, "git-workflow")
	release := filepath.Join(root, "release-checklist")
	if err := os.MkdirAll(filepath.Join(git, "references"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(release, "templates"), 0o755); err != nil {
		return err
	}
	files := map[string][]byte{
		filepath.Join(git, "SKILL.md"):                      []byte("---\nname: git-workflow\ndescription: Follow team Git conventions\n---\n\n# Git\n\nUse feature branches.\n"),
		filepath.Join(git, "notes.md"):                      []byte("# Notes\n"),
		filepath.Join(git, "references", "workflow.svg"):    []byte("<svg>workflow</svg>\n"),
		filepath.Join(git, "references", "meta.json"):       []byte("{\"kind\":\"workflow\"}\n"),
		filepath.Join(git, "icon.unknown"):                  {0, 1, 2, 255},
		filepath.Join(release, "SKILL.md"):                  []byte("---\nname: release-checklist\ndescription: Prepare a safe release\n---\n\n# Release\n\nReview the changelog.\n"),
		filepath.Join(release, "templates", "checklist.md"): []byte("# Checklist\n"),
	}
	archives := filepath.Join(root, "archives")
	if err := os.MkdirAll(archives, 0o755); err != nil {
		return err
	}
	archiveFiles := map[string]string{
		"archive-zip.zip":      "UEsDBBQAAAAIAAAAIVADubX0VgAAAGoAAAAIAAAAU0tJTEwubWQ1y80NgCAMQOF7p2jiuQ7AzSEcoIEqDYgE0Bin1/hzft8jIki8iEEu1usudGoGJ9UWzU3XZHB4g8O74KRH24pgDRoj0H1D9wuAsQo2L5jZBp6/5aE9XFBLAQIUAxQAAAAIAAAAIVADubX0VgAAAGoAAAAIAAAAAAAAAAAAAACkAQAAAABTS0lMTC5tZFBLBQYAAAAAAQABADYAAAB8AAAAAAA=",
		"archive-targz.tar.gz": "H4sIAAAAAAAC/+3UuwrCMBiG4cy5ioBzS1uqQjdBB1EXxQsIbdRQT7RVxKu3VrTQxcmi+D7LH/LnAIEvi8l4OnV3ifggr9QLw6qWmtXze/X4Md/3w0AoT7TglBc6K68U/8lxHLnXOxMpncUbezZO+Rzrq0xMHmf2WNjDPlKDRytRVU+t7KU4ZUblqd1u5f0E2XmukXKZG1VsjDrqONXr16ZqsSsFvsp8NBjORp/9AN7l3wv8Zv4Dv0v+29C5x9OtQ01AAQAAAAAAAAAAAAAAfssNeaEIQgAoAAA=",
		"archive-tgz.tgz":      "H4sIAAAAAAAC/+3TywrCMBCF4azzFAHXLa1WC+5cit2JDxDaqEFbJU1FfHrrFXHvDf9vMyEzw6zOdDLOsrAsxAtFrUGSnGvruUZxv3t/X/7TOOkJFYk3aGqvXXtS/KcgCGSlSzNU2uVLuzOBXxxkYerc2a23m2qoRpdGodqOmtu9b5xR9cqu1/K0LTu3CSlntVF+adRW5yu9uK6cR0Mp8IXmG1dqH/q9/1z+ozh5zn+axuT/HR4yTUIBAAAAAAAAAAAAAAB+0BHJLb/jACgAAA==",
	}
	for path, contents := range files {
		if err := os.WriteFile(path, contents, 0o644); err != nil {
			return err
		}
	}
	for name, encoded := range archiveFiles {
		contents, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(archives, name), contents, 0o644); err != nil {
			return err
		}
	}

	srv := mcp.NewServer(mcp.ServerInfo{Name: "skills-fixture", Version: "1"})
	for _, archive := range []struct{ skillPath, file string }{
		{"archive-targz", "archive-targz.tar.gz"},
		{"archive-tgz", "archive-tgz.tgz"},
		{"archive-zip", "archive-zip.zip"},
	} {
		if err := srv.Skill(archive.skillPath).FromArchive(filepath.Join(archives, archive.file)); err != nil {
			return err
		}
	}
	if err := srv.SkillsFromDir(root); err != nil {
		return err
	}
	srv.SkillTemplate("skill://docs/{product}/SKILL.md", "Product documentation").
		Handler(func(_ context.Context, _ string, params map[string]string) (*mcp.ResourceContent, error) {
			return &mcp.ResourceContent{
				MimeType: mcp.SkillMarkdownMIME,
				Text:     "# Product\n\n" + params["product"] + "\n",
			}, nil
		})
	if err := srv.Err(); err != nil {
		return err
	}
	return mcp.ServeHTTP(ctx, srv, address)
}
