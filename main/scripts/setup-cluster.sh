#!/bin/bash
set -e

echo "Creating namespace..."
kubectl create ns course-reg-exp --dry-run=client -o yaml | kubectl apply -f -

echo "Deploying NATS..."
kubectl apply -f infrastructure/kubernetes/statefulsets/nats.yaml

echo "Installing Scylla Operator..."
helm repo add scylla-operator https://scylla-operator-charts.storage.googleapis.com/stable
helm repo update
helm upgrade --install scylla-operator scylla-operator/scylla-operator --namespace scylla-operator --create-namespace
kubectl apply -f infrastructure/kubernetes/statefulsets/scylladb.yaml

echo "Deploying Dragonfly..."
kubectl apply -f infrastructure/kubernetes/statefulsets/dragonfly.yaml

echo "Setting up k6-operator..."
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
helm upgrade --install k6-operator grafana/k6-operator --namespace k6-operator --create-namespace

echo "Cluster setup complete."