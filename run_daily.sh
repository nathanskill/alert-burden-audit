#!/bin/bash
# run_daily.sh — launchd wrapper for the alert-burden collector (REF-2026-017).
# Runs the daily pull, logs to logs/, and uses an atomic mkdir lock (portable
# to macOS, which lacks flock) so a slow pull never collides with the next
# scheduled invocation.
#
# Scheduled via launchd (StartCalendarInterval: 07:17, 12:17, 18:17 and
# 23:17 local, plus RunAtLoad; see launchd.daily.plist.template and README).
set -uo pipefail
# Self-locate so no machine-specific path is hardcoded (safe to publish).
REPO="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO" || exit 1
mkdir -p logs
LOCKDIR="$REPO/.daily.lockdir"
LOG="$REPO/logs/daily_$(date -u +%Y%m).log"

# Atomic lock: mkdir succeeds only if the dir does not exist. The owning PID
# is written into the lock; a held lock is reclaimed only when that owner is
# demonstrably dead (kill -0 fails), never on age alone while the owner may
# still be mid-backfill. The age fallback (12 h, well above any observed
# backfill duration) applies only when no owner PID was recorded.
if ! mkdir "$LOCKDIR" 2>/dev/null; then
  OWNER="$(cat "$LOCKDIR/pid" 2>/dev/null || true)"
  if [ -n "$OWNER" ] && kill -0 "$OWNER" 2>/dev/null; then
    echo "$(date -u +%FT%TZ) another run in progress (pid $OWNER); skipping" >> "$LOG"
    exit 0
  fi
  if [ -z "$OWNER" ] && [ -z "$(find "$LOCKDIR" -maxdepth 0 -mmin +720 2>/dev/null)" ]; then
    echo "$(date -u +%FT%TZ) lock held, owner unknown and lock young; skipping" >> "$LOG"
    exit 0
  fi
  echo "$(date -u +%FT%TZ) reclaiming stale lock (owner ${OWNER:-unrecorded} not running)" >> "$LOG"
  rm -rf "$LOCKDIR"
  mkdir "$LOCKDIR" 2>/dev/null || { echo "$(date -u +%FT%TZ) lock race; skip" >> "$LOG"; exit 0; }
fi
echo "$$" > "$LOCKDIR/pid"
trap 'rm -rf "$LOCKDIR" 2>/dev/null || true' EXIT

{
  echo "===== $(date -u +%FT%TZ) daily run ====="
  "$REPO/.venv/bin/python" "$REPO/src/collector.py" --daily
  echo "----- exit $? -----"

  # Publish the artifacts/ mirror after every run so the README's claim that
  # artifacts/ is the public record stays continuously true. Non-interactive:
  # the origin remote uses an SSH deploy key; failures are logged, never fatal
  # (the next run retries the push).
  if [ -n "$(git -C "$REPO" status --porcelain -- artifacts 2>/dev/null)" ]; then
    git -C "$REPO" add artifacts &&
    git -C "$REPO" commit -m "artifacts: mirror update $(date -u +%FT%TZ)" -- artifacts &&
    git -C "$REPO" push origin main || echo "artifact publish failed (will retry next run)"
  else
    echo "artifacts/ unchanged; nothing to publish"
  fi
} >> "$LOG" 2>&1
