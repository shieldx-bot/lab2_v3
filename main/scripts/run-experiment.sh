#!/bin/bash
METHOD=${1:-M4}
VUS=${2:-5000}
SCENARIO=${3:-C2}
SEED=${4:-42}

echo "Running experiment: method=$METHOD, vus=$VUS, scenario=$SCENARIO, seed=$SEED"

# Update configmap
kubectl create configmap experiment-config -n course-reg-exp \
  --from-literal=method=$METHOD \
  --from-literal=seed=$SEED \
  --dry-run=client -o yaml | kubectl apply -f -

# Restart deployments to pick up new config
kubectl rollout restart deployment -n course-reg-exp

# Wait for readiness
kubectl wait --for=condition=available --timeout=120s deployment/api-gateway -n course-reg-exp

# Generate load test manifest from template
sed -e "s/{{VUS}}/$VUS/g" -e "s/{{SCENARIO}}/$SCENARIO/g" -e "s/{{METHOD}}/$METHOD/g" \
  load-tests/k6-operator/testrun-base.yaml | kubectl apply -f -

echo "Experiment started. Monitor with Grafana."