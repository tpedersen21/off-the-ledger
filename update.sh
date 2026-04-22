#!/usr/bin/env bash
# One-command update: fetch fresh data, then open the ledger in the default browser.
set -e
cd "$(dirname "$0")"
python3 fetch_data.py
# Mac default open
if command -v open &> /dev/null; then
  open index.html
fi
