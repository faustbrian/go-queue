#!/usr/bin/env bash
set -euo pipefail

part="${1:-}"
case "$part" in
  patch|minor|major) ;;
  *)
    echo "usage: scripts/release.sh <patch|minor|major>" >&2
    exit 2
    ;;
esac

root="$(git rev-parse --show-toplevel)"
cd "$root"

if [[ "$(git branch --show-current)" != "main" ]]; then
  echo "releases must be created from main" >&2
  exit 1
fi
if ! git diff --quiet || ! git diff --cached --quiet ||
  [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  echo "release requires a clean working tree" >&2
  exit 1
fi
if ! git rev-parse --quiet --verify origin/main >/dev/null ||
  [[ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]]; then
  echo "release requires main to match origin/main" >&2
  exit 1
fi

current="v0.0.0"
while IFS= read -r tag; do
  if [[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    current="$tag"
    break
  fi
done < <(git tag --list "v*" --sort=-version:refname)

next="$(go run ./cmd/semvercheck next "$part" "$current")"
version="${next#v}"
if git rev-parse --quiet --verify "refs/tags/$next" >/dev/null; then
  echo "tag $next already exists" >&2
  exit 1
fi
if ! grep -Eq "^## \[$version\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$" CHANGELOG.md; then
  echo "CHANGELOG.md must contain a dated [$version] release section" >&2
  exit 1
fi
if ! grep -Fq "[$version]: https://github.com/faustbrian/go-queue/releases/tag/$next" CHANGELOG.md; then
  echo "CHANGELOG.md must contain the $next release link" >&2
  exit 1
fi

go run ./cmd/semvercheck "$next"
test -z "$(gofmt -l .)"
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.6.1 ./...
go test -race ./...
scripts/check-coverage.sh
scripts/check-docs.sh
make benchmark
go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.7

if ! git diff --quiet || ! git diff --cached --quiet ||
  [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  echo "release checks modified the working tree" >&2
  exit 1
fi

git tag -a "$next" -m "$next"
echo "created local annotated tag $next"
echo "review it, then push with: git push origin $next"
