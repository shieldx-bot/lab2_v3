# Huong dan build va deploy custom scheduler (RedisMetricPlugin)

Tai lieu nay mo ta quy trinh tu build image den deploy va kiem tra custom scheduler `my-scheduler`.

## 1) Chuan bi

- Cluster k3d dang chay (vi du: `k3d cluster list` thay cluster `node`).
- Co quyen `kubectl` tren cluster.
- Redis endpoint co the truy cap tu scheduler pod.

Cac file cau hinh lien quan:
- `scheduler-config.yaml`: cau hinh profile scheduler va plugin
- `scheduler-deploy.yaml`: Deployment cua scheduler plugin

## 2) Build image scheduler

Chay tai thu muc goc scheduler-plugins:

```bash
cd LAB_CENTER/Lab2/scheduler-plugins
make local-image
```

Import image vao k3d (vi du cluster ten `node`):

```bash
k3d image import localhost:5000/scheduler-plugins/kube-scheduler:v20260418- -c node
```

## 3) Deploy scheduler config va deployment

Chay tai thu muc redismetric:

```bash
cd LAB_CENTER/Lab2/scheduler-plugins/pkg/redismetric

kubectl delete cm scheduler-config -n kube-system --ignore-not-found
kubectl create configmap scheduler-config -n kube-system --from-file=scheduler-config.yaml=scheduler-config.yaml
kubectl create serviceaccount scheduler-plugin -n kube-system

kubectl apply -f scheduler-deploy.yaml
kubectl rollout status deploy/scheduler-plugin -n kube-system --timeout=180s
```
Kiem tra pod scheduler:

```bash
kubectl get pods -n kube-system -o wide | grep scheduler-plugin



kubectl get pods -n kube-system -l component=scheduler -o wide
```

## 4) Kiem tra custom scheduler hoat dong

Tao namespace test va pod su dung `schedulerName: my-scheduler`:

```bash
kubectl create ns test --dry-run=client -o yaml | kubectl apply -f -

cat <<'EOF' | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: custom-scheduler-e2e
  namespace: scheduler-test
spec:
  schedulerName: my-scheduler
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.9
EOF
```

Kiem tra event scheduler:

```bash
kubectl describe pod custom-scheduler-e2e -n scheduler-test
kubectl get events -n scheduler-test --field-selector involvedObject.name=custom-scheduler-e2e --sort-by=.lastTimestamp
```

Dau hieu thanh cong:
- Event co dong: `From: my-scheduler`
- Message: `Successfully assigned ...`

## 5) Kiem tra plugin RedisMetricPlugin da duoc goi

```bash
kubectl logs -n kube-system -l component=scheduler --since=5m | grep -E 'RedisMetricPlugin|Successfully bound pod|Attempting to schedule pod'
```

Neu thay dong `RedisMetricPlugin da ket noi thanh cong den Redis` va log scheduling cho pod test, plugin da duoc kich hoat.

## 6) Luu y van de Redis key TOP

Plugin hien tai doc Redis bang `HGETALL TOP` (TOP phai la HASH). Neu log bao:

- `WRONGTYPE Operation against a key holding the wrong kind of value`

thi key `TOP` dang khong phai HASH. Khi do scheduler van co the bind pod, nhung diem score tu Redis se khong dung y do.

Vi du tao HASH dung dinh dang:

```bash
redis-cli -h <redis-host> -p 6379 -a <password> HSET TOP k3d-node-server-0 1 k3d-node-agent-0 1
```

## 7) Don dep test resources

```bash
kubectl delete pod custom-scheduler-e2e -n scheduler-test --ignore-not-found
kubectl delete pod default-scheduler-e2e -n scheduler-test --ignore-not-found
kubectl delete ns scheduler-test --ignore-not-found
```
