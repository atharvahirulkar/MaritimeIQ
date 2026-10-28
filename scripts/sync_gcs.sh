#!/usr/bin/env bash
# Sync lakehouse + eval artifacts to GCS.
# Usage: GCS_BUCKET=gs://my-bucket/maritimeiq ./scripts/sync_gcs.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUCKET="${GCS_BUCKET:-}"
if [[ -z "$BUCKET" ]]; then
  echo "Set GCS_BUCKET=gs://your-bucket/path" >&2
  exit 1
fi
if ! command -v gsutil >/dev/null 2>&1; then
  echo "gsutil not found. Install Google Cloud SDK." >&2
  exit 1
fi
echo "Syncing lakehouse → $BUCKET/lakehouse"
gsutil -m rsync -r "$ROOT/lakehouse" "$BUCKET/lakehouse"
if [[ -d "$ROOT/.eval_results" ]]; then
  echo "Syncing eval → $BUCKET/eval_results"
  gsutil -m rsync -r "$ROOT/.eval_results" "$BUCKET/eval_results"
fi
echo "Done."
