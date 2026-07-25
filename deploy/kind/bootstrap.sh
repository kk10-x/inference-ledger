#!/usr/bin/env bash
# Stand up the whole system on a local kind cluster.
#
#   PROVIDER_API_KEY=sk-... ./deploy/kind/bootstrap.sh
#
# Idempotent: re-running reuses the cluster and upgrades the release. Requires
# docker, kind, kubectl and helm on PATH.
set -euo pipefail

CLUSTER=inference-ledger
RELEASE=il
IMAGE_TAG=0.1.0
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

echo "==> cluster"
if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --config "$ROOT/deploy/kind/kind-config.yaml"
fi

echo "==> build image"
docker build -t "inference-ledger:${IMAGE_TAG}" -f "$ROOT/docker/Dockerfile" "$ROOT"

echo "==> load image into the cluster"
# kind nodes cannot see the host daemon's images; they must be sideloaded.
kind load docker-image "inference-ledger:${IMAGE_TAG}" --name "$CLUSTER"

echo "==> install/upgrade release"
helm upgrade --install "$RELEASE" "$ROOT/deploy/helm/inference-ledger" \
  --set-string provider.apiKey="${PROVIDER_API_KEY:-}" \
  --wait --timeout 5m

echo "==> rollout"
kubectl rollout status "deploy/${RELEASE}-gateway" --timeout 3m
kubectl rollout status "deploy/${RELEASE}-reconciler" --timeout 3m

cat <<EOF

Up. Reach the gateway (loopback only):
  kubectl port-forward svc/${RELEASE}-gateway 8080:8080

Tear down:
  kind delete cluster --name ${CLUSTER}
EOF
