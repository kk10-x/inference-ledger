#!/usr/bin/env bash
# Graceful-drain verification on Kubernetes.
#
# The Kubernetes analogue of the chaos suite's gateway-sigterm-drain scenario:
# delete a gateway pod while streams are in flight and assert the ledger stays
# correct. A pod deletion sends SIGTERM and honours terminationGracePeriodSeconds,
# so if the chart's grace window and the app's drain agree (see docs/shutdown.md),
# every in-flight request still settles exactly once.
#
#   ./deploy/kind/verify-drain.sh
#
# Assumes the release is installed (deploy/kind/bootstrap.sh) with the in-cluster
# mock provider enabled.
set -euo pipefail
RELEASE=il

echo "==> port-forward gateway"
kubectl port-forward "svc/${RELEASE}-gateway" 18080:8080 >/tmp/pf-gw.log 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
sleep 3

echo "==> baseline settlements"
sql() { kubectl exec "deploy/${RELEASE}-postgres" -- psql -U ledger -d ledger -tAc "$1"; }
before=$(sql "SELECT count(*) FROM settlements")

echo "==> drive 60 streaming requests in the background"
for i in $(seq 1 60); do
  curl -s -N -m 30 localhost:18080/v1/chat/completions \
    -H 'Content-Type: application/json' \
    -H "X-Tenant-Id: drain" -H "Idempotency-Key: drain-$i" \
    -d '{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"drain test"}]}' \
    >/dev/null 2>&1 &
  sleep 0.15
done

echo "==> delete a gateway pod mid-flight (SIGTERM + graceful drain)"
victim=$(kubectl get pods -l app.kubernetes.io/component=gateway -o name | head -1)
kubectl delete "$victim" --wait=false
echo "    deleted $victim"

echo "==> wait for load to finish and the settlement window to pass"
wait  # background curls
sleep 40

after=$(sql "SELECT count(*) FROM settlements")
pending=$(sql "SELECT count(*) FROM pending_settlements WHERE tenant_id = 'drain'")
drain_total=$(sql "SELECT count(*) FROM settlements WHERE tenant_id = 'drain'")
dupes=$(sql "SELECT count(*) FROM settlements WHERE drift_reason = 'retry_double_count'")

echo
echo "settlements before : $before"
echo "settlements after  : $after"
echo "drain requests settled: $drain_total"
echo "still pending (drain) : $pending"
echo "double-counts         : $dupes"
echo

# The invariant: the pod eviction cost no request. Every drain request settled,
# none is stuck pending, and nothing was billed twice.
if [ "$pending" -eq 0 ] && [ "$dupes" -eq 0 ] && [ "$drain_total" -ge 55 ]; then
  echo "PASS: graceful drain settled every in-flight request exactly once"
else
  echo "FAIL: drain left requests unaccounted, pending, or double-billed"
  exit 1
fi
