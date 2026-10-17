#!/bin/bash
# `swift test` that also works with only the Command Line Tools (no Xcode).
# The CLT ship Swift Testing, but SwiftPM does not add its framework and
# library paths, so the tests fail with "no such module 'Testing'".
# Usage: Scripts/test.sh [swift test arguments, e.g. --filter FixtureTests]
set -euo pipefail
cd "$(dirname "$0")/.."

args=()
if [ "$(uname)" = "Darwin" ]; then
  dev="$(xcode-select -p 2>/dev/null)"
  case "$dev" in
    */CommandLineTools)
      d="$dev/Library/Developer"
      # A separate build directory, so plain `swift build` does not rebuild
      # everything after a test run (and the other way round).
      args=(
        --scratch-path .build/clt-test
        -Xswiftc -F -Xswiftc "$d/Frameworks"
        -Xlinker -F -Xlinker "$d/Frameworks"
        -Xlinker -rpath -Xlinker "$d/Frameworks"
        -Xlinker -rpath -Xlinker "$d/usr/lib"
      )
      ;;
  esac
fi

exec swift test ${args[@]+"${args[@]}"} "$@"
