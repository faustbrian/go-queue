package queue

import (
	"bufio"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const repositoryName = "go-queue"

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
		"GOAL.md",
		"GOAL_HARDEN.md",
		"LICENSE",
		"Makefile",
		"NOTICE",
		"README.md",
		"ROADMAP.md",
		"SECURITY.md",
		"THIRD_PARTY_NOTICES.md",
		"llms.txt",
		"llms-full.txt",
		"docs/README.md",
		"docs/quickstart.md",
		"docs/adoption.md",
		"docs/api.md",
		"docs/architecture.md",
		"docs/examples.md",
		"docs/cookbook.md",
		"docs/faq.md",
		"docs/troubleshooting.md",
		"docs/migration.md",
		"docs/compatibility.md",
		"docs/performance.md",
		"docs/hardening.md",
		"docs/security.md",
		"docs/releasing.md",
		"docs/repository-standards.md",
		".github/workflows/ci.yml",
		".github/workflows/benchmark.yml",
		".github/workflows/fuzz.yml",
		".github/workflows/security.yml",
		".github/workflows/release.yml",
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

	_, integrationErr := os.Stat(".github/workflows/integration.yml")
	if repositoryName == "go-queue" && integrationErr != nil {
		t.Errorf("go-queue integration workflow: %v", integrationErr)
	}
	if repositoryName != "go-queue" && integrationErr == nil {
		t.Error("integration workflow is only approved for go-queue")
	}
	if integrationErr != nil && !errors.Is(integrationErr, os.ErrNotExist) {
		t.Errorf("stat integration workflow: %v", integrationErr)
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

func TestSharedToolingContract(t *testing.T) {
	t.Parallel()

	required := map[string][]string{
		"go.mod": {"go 1.25.0"},
		"AGENTS.md": {
			"meaningful 100% coverage",
			"## Repository-Specific Rules",
			"CHANGELOG.md",
		},
		"CLAUDE.md":       {"AGENTS.md"},
		"README.md":       {"Go 1.25 or later", "llms.txt", "llms-full.txt", "CHANGELOG.md"},
		"CONTRIBUTING.md": {"Go 1.25 or later", "make check", "CHANGELOG.md"},
		"Makefile": {
			"BENCH_TIME ?= 100x",
			"format:",
			"format-check:",
			"test:",
			"test-race:",
			"coverage:",
			"vet:",
			"lint:",
			"fuzz:",
			"benchmark:",
			"docs:",
			"vuln:",
			"check:",
			"release-patch:",
			"release-minor:",
			"release-major:",
		},
		"llms.txt":                  {"# " + repositoryName, "llms-full.txt", "docs/quickstart.md"},
		"scripts/check-coverage.sh": {"100.0%"},
		"scripts/check-docs.sh":     {"relative Markdown links", "generate-llms.py --check"},
		"scripts/generate-llms.py":  {"README.md", "--check"},
		"scripts/release.sh":        {"origin/main", "make check", "git tag -a"},
		".github/workflows/ci.yml": {
			"make format-check",
			"make vet",
			"make test-race",
			"make coverage",
			"make docs",
		},
		".github/workflows/benchmark.yml": {"make benchmark BENCH_TIME=100x", "upload-artifact"},
		".github/workflows/fuzz.yml":      {"make fuzz FUZZ_TIME=30s"},
		".github/workflows/security.yml":  {"govulncheck-action", "dependency-review-action"},
		".github/workflows/release.yml": {
			`"v*.*.*"`,
			"merge-base --is-ancestor",
			"make test-race",
			"gh release create",
			"sha256sum",
		},
		".github/dependabot.yml": {"gomod", "github-actions"},
	}

	for path, fragments := range required {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, fragment := range fragments {
			if !strings.Contains(string(contents), fragment) {
				t.Errorf("%s does not contain %q", path, fragment)
			}
		}
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
