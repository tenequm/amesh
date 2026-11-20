#!/usr/bin/env bash
set -euo pipefail

revision=$(git rev-parse "$1^{commit}")
tree=$(git rev-parse "$revision^{tree}")
if parent=$(git rev-parse --verify -q "$revision^2") && [[ "$(git rev-parse "$parent^{tree}")" == "$tree" ]]; then
    revision=$parent
fi

run=$(gh api "repos/$GITHUB_REPOSITORY/actions/workflows/ci.yml/runs?event=push&head_sha=$revision&per_page=1" \
    --jq '.workflow_runs[0] | select(.conclusion == "success") | .id')
[[ -n "$run" ]]
gh api "repos/$GITHUB_REPOSITORY/actions/runs/$run/jobs" | jq -e '
    [.jobs[] | select(.conclusion == "success") | .name] |
    index("check (ubuntu-latest)") != null and index("check (macos-latest)") != null' >/dev/null
echo "Both OS checks passed for tree $tree in run $run."
