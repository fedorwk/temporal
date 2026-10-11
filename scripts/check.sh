#!/usr/bin/env bash
# Exit gate for every session: formatting, vet, race tests, debug-assertion tests.
set -euo pipefail
cd "$(dirname "$0")/.."

unformatted=$(gofmt -l .)
if [[ -n "$unformatted" ]]; then
  echo "gofmt needed on:"
  echo "$unformatted"
  exit 1
fi

go vet ./...
go vet -tags temporaldebug ./...
go test -race ./...
go test -tags temporaldebug ./...
echo "check: OK"
