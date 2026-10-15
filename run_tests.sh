#!/usr/bin/env bash
# Run the task-workspace test suite against an installed AstrBot interpreter.
# No package installation is performed.
#
# Set ASTRBOT_PYTHON to a Python interpreter that can `import astrbot`.
# When unset, common interpreter names are probed for importability.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

detect_python() {
  if [ -n "${ASTRBOT_PYTHON:-}" ]; then
    printf '%s' "$ASTRBOT_PYTHON"
    return 0
  fi
  local candidate
  for candidate in python3 python; do
    if command -v "$candidate" >/dev/null 2>&1 \
       && "$candidate" -c "import astrbot" >/dev/null 2>&1; then
      printf '%s' "$candidate"
      return 0
    fi
  done
  return 1
}

PY="$(detect_python)" || {
  echo "ERROR: no Python interpreter with 'import astrbot' found." >&2
  echo "Install AstrBot or set ASTRBOT_PYTHON to its interpreter." >&2
  exit 2
}

# Keep AstrBot from creating runtime data inside the source tree.
export ASTRBOT_ROOT="${ASTRBOT_ROOT:-$(mktemp -d)}"
export TASK_WORKSPACE_DATA_DIR="${TASK_WORKSPACE_DATA_DIR:-$(mktemp -d)}"
exec "$PY" -m unittest discover -s tests -t tests -v "$@"
