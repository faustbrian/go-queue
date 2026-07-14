#!/usr/bin/env bash
set -euo pipefail

required=(
	README.md
	CONTRIBUTING.md
	SECURITY.md
	docs/architecture.md
	docs/api.md
	docs/backend-support.md
	docs/delivery-semantics.md
	docs/migration.md
	docs/compatibility.md
	docs/adoption.md
	docs/cookbook.md
	docs/faq.md
	docs/troubleshooting.md
	docs/releases.md
)

for file in "${required[@]}"; do
	[[ -s "$file" ]] || { printf 'missing required document: %s\n' "$file" >&2; exit 1; }
done

while IFS= read -r file; do
	while IFS= read -r target; do
		target=${target%%#*}
		[[ -z "$target" || "$target" == http://* || "$target" == https://* || "$target" == mailto:* ]] && continue
		path=$(dirname "$file")/$target
		[[ -e "$path" ]] || { printf 'broken link in %s: %s\n' "$file" "$target" >&2; exit 1; }
	done < <(sed -nE 's/.*\]\(([^)]+)\).*/\1/p' "$file")
done < <(find . -name '*.md' -not -path './.git/*' -print)

go test ./examples/...
