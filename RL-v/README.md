# lab2

kind create cluster --name multi-node-demo --config cluster.yaml


docker update --cpus="4" --memory="8g" --memory-swap="8g" multi-node-demo-control-plane
docker update --cpus="2" --memory="4g" --memory-swap="4g" multi-node-demo-worker
docker update --cpus="2" --memory="4g" --memory-swap="4g" multi-node-demo-worker2
docker update --cpus="2" --memory="4g" --memory-swap="4g" multi-node-demo-worker3
docker update --cpus="2" --memory="4g" --memory-swap="4g" multi-node-demo-worker4
docker update --cpus="2" --memory="4g" --memory-swap="4g" multi-node-demo-worker5


kubectl get all -A -o yaml > cluster-backup.yaml
kind delete cluster --name multi-node-demo