#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="pong-metrics"
NAMESPACE="monitoring"

echo "=== Creating k3d cluster ==="
if k3d cluster list | grep -q "$CLUSTER_NAME"; then
  echo "Cluster '$CLUSTER_NAME' already exists, skipping creation"
else
  k3d cluster create "$CLUSTER_NAME" \
    -p "4317:30317@server:0" \
    -p "3000:30300@server:0"
fi

echo ""
echo "=== Adding Helm repos ==="
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts 2>/dev/null || true
helm repo update

echo ""
echo "=== Installing kube-prometheus-stack ==="
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  -n "$NAMESPACE" --create-namespace \
  -f "$(dirname "$0")/kube-prometheus-values.yaml" \
  --wait --timeout 5m

echo ""
echo "=== Installing OpenTelemetry Collector ==="
helm upgrade --install otel-collector open-telemetry/opentelemetry-collector \
  -n "$NAMESPACE" \
  -f "$(dirname "$0")/otel-collector-values.yaml" \
  --wait --timeout 3m

echo ""
echo "=== Patching OTel Collector service to NodePort 30317 ==="
kubectl patch svc otel-collector-opentelemetry-collector -n "$NAMESPACE" \
  -p '{"spec":{"type":"NodePort","ports":[{"port":4317,"targetPort":4317,"nodePort":30317,"protocol":"TCP","name":"otlp-grpc"}]}}'

echo ""
echo "=== Waiting for pods ==="
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=grafana -n "$NAMESPACE" --timeout=120s
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=prometheus -n "$NAMESPACE" --timeout=120s

echo ""
echo "=== Connection Info ==="
echo "Grafana:        http://localhost:3000  (admin / admin)"
echo "OTel Collector: localhost:4317 (OTLP gRPC)"
echo ""
echo "Test scrape:    python scraper/scraper.py --start 1170 --end 1172 --endpoint localhost:4317"
echo "Full backfill:  python scraper/scraper.py --start 1 --end 1172 --endpoint localhost:4317"
