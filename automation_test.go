package queue

import (
	"os"
	"strings"
	"testing"
)

func TestRepositoryAutomationContract(t *testing.T) {
	t.Parallel()

	required := map[string][]string{
		"CLAUDE.md": {"@AGENTS.md"},
		"Makefile": {
			"BENCH_TIME ?= 100x",
			"fuzz",
			"release-patch",
			"release-minor",
			"release-major",
			"scripts/release.sh",
		},
		"llms.txt": {
			"# go-queue",
			"llms-full.txt",
			"docs/architecture.md",
		},
		"llms-full.txt": {
			"# go-queue",
			"# Architecture",
		},
		"README.md": {"llms.txt", "llms-full.txt", "CHANGELOG.md"},
		".github/workflows/ci.yml": {
			"go test -race ./...",
			"scripts/check-coverage.sh",
			"staticcheck ./...",
			"go vet ./...",
			"gofmt",
			"scripts/check-docs.sh",
			"actionlint@v1.7.7",
		},
		".github/workflows/fuzz.yml":        {"scripts/check-fuzz.sh"},
		".github/workflows/integration.yml": {"integration"},
		".github/workflows/benchmark.yml": {
			"make benchmark",
			"upload-artifact",
		},
		".github/workflows/security.yml": {"govulncheck", "dependency-review"},
		".github/workflows/release.yml": {
			"tags:",
			`"v*"`,
			"go run ./cmd/semvercheck",
			"merge-base --is-ancestor",
			"gh release create",
		},
		".github/dependabot.yml":    {"gomod", "github-actions"},
		"scripts/check-coverage.sh": {"100.0%"},
		"scripts/check-fuzz.sh": {
			"FuzzDecodeE",
			"FuzzMessageValidation",
			"FuzzRequestDelivery",
		},
		"scripts/check-docs.sh": {
			"go test ./...",
			"Markdown link",
			"generate-llms.py --check",
		},
		"scripts/generate-llms.py": {"README.md", "--check"},
		"scripts/release.sh": {
			"git tag -a",
			"origin/main",
			"scripts/check-coverage.sh",
			"make benchmark",
		},
	}

	for path, fragments := range required {
		path, fragments := path, fragments
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile(%q) error = %v", path, err)
			}
			for _, fragment := range fragments {
				if !strings.Contains(string(contents), fragment) {
					t.Errorf("%s does not contain %q", path, fragment)
				}
			}
		})
	}
}

func TestRepositoryRequiresGo12512(t *testing.T) {
	t.Parallel()

	required := map[string]string{
		"go.mod":          "go 1.25.12",
		"README.md":       "Go 1.25.12 or newer",
		"CONTRIBUTING.md": "Go 1.25.12 or newer",
	}
	for path, fragment := range required {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%q) error = %v", path, err)
		}
		if !strings.Contains(string(contents), fragment) {
			t.Errorf("%s does not contain %q", path, fragment)
		}
	}
}
