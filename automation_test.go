package queue

import (
	"bufio"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestSharedRepositoryContract(t *testing.T) {
	t.Parallel()

	required := []string{
		".gitattributes",
		".gitignore",
		".golangci.yml",
		"AGENTS.md",
		"CHANGELOG.md",
		"CLAUDE.md",
		"CODE_OF_CONDUCT.md",
		"CONTRIBUTING.md",
		"LICENSE",
		"Makefile",
		"NOTICE",
		"README.md",
		"ROADMAP.md",
		"SECURITY.md",
		"THIRD_PARTY_NOTICES.md",
		"docs/README.md",
		"docs/quickstart.md",
		"docs/adoption.md",
		"docs/api.md",
		"docs/architecture.md",
		"docs/go-safety-and-concurrency.md",
		"docs/examples.md",
		"docs/cookbook.md",
		"docs/faq.md",
		"docs/troubleshooting.md",
		"docs/migration.md",
		"docs/compatibility.md",
		"docs/performance.md",
		"docs/security.md",
		"docs/releasing.md",
		"docs/repository-standards.md",
	}

	for _, path := range required {
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("required repository file %q: %v", path, err)
			continue
		}
		if info.Size() == 0 {
			t.Errorf("required repository file %q is empty", path)
		}
	}

}

func TestSharedDocumentationConventions(t *testing.T) {
	t.Parallel()

	err := filepath.WalkDir("docs", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}
		base := filepath.Base(path)
		if base != "README.md" && base != strings.ToLower(base) {
			t.Errorf("documentation filename must be lowercase kebab-case: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	headings := []string{
		"## Status",
		"## Requirements",
		"## Installation",
		"## Quickstart",
		"## Package Guarantees",
		"## Documentation",
		"## Development",
		"## Contributing",
		"## Security",
		"## License",
	}
	position := -1
	for _, heading := range headings {
		next := strings.Index(string(readme), heading)
		if next <= position {
			t.Fatalf("README heading %q is missing or out of order", heading)
		}
		position = next
	}
}

func TestGitHubActionsUseFullCommitSHAs(t *testing.T) {
	t.Parallel()

	pinned := regexp.MustCompile(`^[0-9a-f]{40}$`)
	workflows, err := filepath.Glob(".github/workflows/*.yml")
	if err != nil {
		t.Fatal(err)
	}
	for _, workflow := range workflows {
		file, err := os.Open(workflow)
		if err != nil {
			t.Fatal(err)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if !strings.HasPrefix(line, "uses:") {
				continue
			}
			reference := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, "uses:")))[0]
			parts := strings.Split(reference, "@")
			if len(parts) != 2 || !pinned.MatchString(parts[1]) {
				t.Errorf("%s contains unpinned action reference %q", workflow, reference)
			}
		}
		if err := scanner.Err(); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
