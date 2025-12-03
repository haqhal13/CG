#!/usr/bin/env bash
# Helper script to launch the Polymarket copy bot with a single command.

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/venv"

if [ -z "${PYTHON_BIN:-}" ]; then
  if [ -n "${PYENV_ROOT:-}" ] && [ -x "$PYENV_ROOT/versions/3.11.9/bin/python" ]; then
    PYTHON_BIN="$PYENV_ROOT/versions/3.11.9/bin/python"
  elif [ -x "$HOME/.pyenv/versions/3.11.9/bin/python" ]; then
    PYTHON_BIN="$HOME/.pyenv/versions/3.11.9/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi

if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment at $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

echo "Installing/upgrading dependencies..."
"$VENV_DIR/bin/python" -m pip install --upgrade pip >/dev/null
"$VENV_DIR/bin/pip" install -r "$PROJECT_DIR/requirements.txt" >/dev/null

echo "Starting Polymarket copy bot..."
exec "$VENV_DIR/bin/python" "$PROJECT_DIR/polymarket_copy_bot.py" "$@"
