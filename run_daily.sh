#!/bin/bash
# run_daily.sh — cron wrapper for the alert-burden collector (REF-2026-017).
# Runs the daily pull, logs to logs/, and uses an atomic mkdir lock (portable
# to macOS, which lacks flock) so a slow pull never collides with the next
# scheduled invocation.
#
# Installed via crontab every 6 hours (see 总控台 / README).
set -uo pipefail
# Self-locate so no machine-specific path is hardcoded (safe to publish).
REPO="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO" || exit 1
mkdir -p logs
LOCKDIR="$REPO/.daily.lockdir"
LOG="$REPO/logs/daily_$(date -u +%Y%m).log"

# Atomic lock: mkdir succeeds only if the dir does not exist.
if ! mkdir "$LOCKDIR" 2>/dev/null; then
  # Stale-lock guard: if the lock is older than 3h, a previous run died; reclaim.
  if [ -d "$LOCKDIR" ] && [ -n "$(find "$LOCKDIR" -maxdepth 0 -mmin +180 2>/dev/null)" ]; then
    echo "$(date -u +%FT%TZ) reclaiming stale lock" >> "$LOG"
    rmdir "$LOCKDIR" 2>/dev/null || true
    mkdir "$LOCKDIR" 2>/dev/null || { echo "$(date -u +%FT%TZ) lock race; skip" >> "$LOG"; exit 0; }
  else
    echo "$(date -u +%FT%TZ) another run in progress; skipping" >> "$LOG"
    exit 0
  fi
fi
trap 'rmdir "$LOCKDIR" 2>/dev/null || true' EXIT

{
  echo "===== $(date -u +%FT%TZ) daily run ====="
  "$REPO/.venv/bin/python" "$REPO/src/collector.py" --daily
  echo "----- exit $? -----"
} >> "$LOG" 2>&1
