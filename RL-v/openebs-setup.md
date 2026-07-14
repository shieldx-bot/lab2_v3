# Hướng dẫn cài đặt OpenEBS đầy đủ cho Kubernetes

## Mục đích

Cluster của bạn sử dụng `storageClassName: openebs-jiva-csi-default` và `openebs-hostpath` cho các PVC. Lỗi xảy ra vì **chỉ cài Jiva CSI controller rồi thiếu hostpath provisioner**, khiến tất cả Jiva replica (`*-jiva-rep-*`) bị `Pending` vô thời hạn, toàn bộ workload (nats, redis, metricdb) không thể mount volume.

Tài liệu này đảm bảo bạn cài đủ 2 thành phần: **Jiva CSI** + **LocalPV Hostpath Provisioner**.

---

## Yêu cầu

- Kubernetes >= 1.23
- `kubectl` đã cấu hình
- Quyền cluster-admin
- iSCSI initiator đã cài trên các node: `open-iscsi` (Debian/Ubuntu) hoặc `iscsi-initiator-utils` (RHEL/CentOS)

---

## Bước 1 — Thêm Helm repo OpenEBS

```bash
helm repo add openebs https://openebs.github.io/charts
helm repo update
```

---

## Bước 2 — Cài OpenEBS control plane (Jiva CSI)

Trong hướng dẫn trước, bạn đã có sẵn:

- `jiva-operator`
- `openebs-jiva-csi-controller`
- `openebs-jiva-csi-node` (1 pod/node)

Nếu cần cài lại:

```bash
helm install openebs openebs/openebs \
  --namespace openebs \
  --create-namespace \
  --set jiva.enabled=true \
  --set local.enabled=false \
  --set mayastor.enabled=false
```

Kiểm tra:

```bash
kubectl -n openebs get pods
```

Kỳ vọng: `jiva-operator`, `openebs-jiva-csi-controller`, nhiều `openebs-jiva-csi-node-*` đều `Running`.

---

## Bước 3 — Cài OpenEBS LocalPV Hostpath Provisioner (quan trọng!)

Đây là phần bị thiếu trong lần cài trước. Nó tạo ra các PVC cho Jiva replicas chạy được.

```bash
curl -sfL "https://raw.githubusercontent.com/openebs/charts/gh-pages/hostpath-operator.yaml" \
  | kubectl apply -f -
```

Output kỳ vọng:

```
serviceaccount/openebs-maya-operator created
clusterrole.rbac.authorization.k8s.io/openebs-maya-operator created
clusterrolebinding.rbac.authorization.k8s.io/openebs-maya-operator created
deployment.apps/openebs-localpv-provisioner created
```

---

## Bước 4 — Tạo StorageClass `openebs-hostpath`

```bash
cat > /tmp/openebs-hostpath-sc.yaml <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: 
  name: openebs-hostpath
provisioner: openebs.io/local
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
parameters:
  storageType: hostpath
  basePath: /var/openebs/local
EOF

kubectl apply -f /tmp/openebs-hostpath-sc.yaml
```

Kiểm tra:

```bash
kubectl get storageclass | grep openebs
```

Kỳ vọng thấy 2 SC:
- `openebs-jiva-csi-default` (đã có)
- `openebs-hostpath` (mới tạo)

---

## Bước 5 — Tạo StoragePool (tùy chọn, tối ưu)

Nếu các node có disk riêng cho OpenEBS, tạo pool trỏ vào đường dẫn đó:

```bash
cat > /tmp/openebs-pool.yaml <<'EOF'
apiVersion: openebs.io/v1alpha1
kind: StoragePool
metadata:
  name: default-hostpath
spec:
  type: hostdir
  path: /var/openebs/local
EOF

kubectl apply -f /tmp/openebs-pool.yaml
```

> Mặc định Jiva sẽ dùng sparse file trên rootfs nếu không tạo pool. Chỉ cần `basePath` trong SC là đủ để chạy.

---

## Bước 6 — Cấu hình namespace cho application

Đảm bảo PVC của app đang dùng đúng `storageClassName`:

| Tên PVC trong app | StorageClass cần có |
|-------------------|---------------------|
| `data-redis-*` | `openebs-jiva-csi-default` |
| `jetstream-storage-nats-js-*` | `openebs-jiva-csi-default` |
| `metricdb-*` | `openebs-jiva-csi-default` |

Các PVC tự động sinh từ StatefulSet volumeClaimTemplate sẽ tự dùng storage class đã ghi trong spec — không cần can thiệp thêm nếu đã đúng.

---

## Bước 7 — Verify toàn cluster

```bash
# 1. OpenEBS control plane
kubectl -n openebs get pods

# 2. LocalPV provisioner
kubectl -n openebs get pods | grep provisioner

# 3. Storage classes
kubectl get storageclass | grep openebs

# 4. PVCs trong namespace app
kubectl get pvc -n <namespace>

# 5. Jiva components cho từng PVC
kubectl get pods -n openebs | grep jiva-ctrl
kubectl get pods -n openebs | grep jiva-rep
```

**Quy tắc kiểm tra:**
- `*-jiva-ctrl-*`: 2/2 Running
- `*-jiva-rep-0/1/2`: 1/1 Running
- Nếu replica nào `Pending` → xem events của PVC tương ứng trong `openebs` namespace.

---

## Xử lý sự cố

### Replica Pending do thiếu provisioner

Nếu thấy:

```
pod has unbound immediate PersistentVolumeClaims. not found
```

Kiểm tra provisioner đã chạy chưa:

```bash
kubectl -n openebs get pods | grep provisioner
kubectl get storageclass openebs-hostpath
```

Nếu thiếu → chạy lại Bước 3 và Bước 4.

### Volume "not ready for workloads"

```bash
kubectl describe pod <pod-name> -n <namespace> | grep -A3 "Events:"
kubectl get pods -n openebs | grep <pvc-id-prefix>
```

Thường do:
- ❌ `openebs-hostpath` SC chưa có
- ❌ `openebs-localpv-provisioner` chưa chạy
- ❌ Path `/var/openebs/local` chưa tồn tại trên node → cần tạo thủ công hoặc dùng path khác

```bash
# Tạo thư mục trên tất cả node nếu dùng hostpath trực tiếp
for node in node2 node3 node4 node5; do
  kubectl debug node/$node -it --image=busybox -- mkdir -p /var/openebs/local
done
```

### Pod vẫn lỗi sau khi provisioner chạy

```bash
kubectl delete pod <pod-name> -n <namespace> --grace-period=0 --force
kubectl rollout restart statefulset/<name> -n <namespace>
```

---

## Checklist cài đặt hoàn chỉnh

- [ ] `helm repo add openebs ...` + `helm repo update`
- [ ] `helm install openebs openebs/openebs` với `jiva.enabled=true`
- [ ] `kubectl -n openebs get pods` → tất cả Running
- [ ] `curl .../hostpath-operator.yaml | kubectl apply -f -`
- [ ] Tạo StorageClass `openebs-hostpath` với `provisioner: openebs.io/local`
- [ ] Tạo StoragePool (tùy chọn)
- [ ] `kubectl rollout restart statefulset/<app>` để application nhận SC mới
- [ ] `kubectl get pods -n openebs | grep jiva-rep` → tất cả 1/1 Running
- [ ] App pods đều Ready

---

## Lưu ý quan trọng

1. **Không xóa PVC/StatefulSet khi gặp lỗi** — nó sẽ khiến data bị mất và tạo volume mới hao tốn tài nguyên.
2. **Mỗi Jiva volume cần đúng 3 replica** (`*-jiva-rep-0/1/2`) để đảm bảo HA.
3. **openebs-hostpath SC là bắt buộc** cho Jiva replicas — nếu thiếu, toàn bộ `jiva-rep-*` sẽ `Pending` vô thời hạn.
4. **Dùng `openebs.io/local` provisioner** cho hostpath, không phải `jiva.csi.openebs.io` (cái này dành cho controller PVC của Jiva).
