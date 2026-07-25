#!/usr/bin/env bash
# Keeps the public demo dashboard alive with a gentle, continuous trickle of
# traffic. Meant to be run from cron every 5 minutes; each invocation runs for
# just under the interval so coverage is effectively continuous without ever
# stacking two generators.
#
#   */5 * * * * /home/khrithik/projects/inference-ledger/deploy/demo/trickle-load.sh
#
# Low rate on purpose: this is a demo, not a benchmark. It costs nothing — the
# gateway proxies to the in-cluster mock provider, not a paid API.
set -euo pipefail
cd "$(dirname "$0")/../.."

# Skip if a previous run is somehow still going, so cron overlaps never pile up.
if pgrep -f "[c]haos.load --tenant-prefix demo" >/dev/null; then
  exit 0
fi

PYTHONPATH=src:. .venv-chaos/bin/python -m chaos.load \
  --url http://localhost:8080 \
  --rps 2 \
  --duration 290 \
  --disconnect-rate 0.05 \
  --tenant-prefix demo
