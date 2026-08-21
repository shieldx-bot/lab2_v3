"""
Alternative collector using the Kubernetes Python client directly.
This bypasses kubectl CLI and calls the Metrics API via the API server.
Requires: pip install kubernetes pyyaml
"""

from datetime import datetime, timezone
from kubernetes import client, config as k8s_config


def _load_config():
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()


def collect_node_metrics_api() -> list[dict]:
    _load_config()
    api = client.CustomObjectsApi()

    resources = api.list_cluster_custom_object(
        group="metrics.k8s.io",
        version="v1beta1",
        plural="nodes",
    )

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for item in resources.get("items", []):
        meta = item.get("metadata", {})
        usage = item.get("usage", {})

        cpu_str = usage.get("cpu", "0")
        mem_str = usage.get("memory", "0")

        rows.append({
            "timestamp": now,
            "node_name": meta.get("name", ""),
            "cpu_requested": cpu_str,
            "cpu_percent_requested": "",
            "mem_requested": mem_str,
            "mem_percent_requested": "",
            "cpu_raw_nano": _cpu_to_nano(cpu_str),
            "mem_raw_bytes": _mem_to_bytes(mem_str),
        })
    return rows


def _cpu_to_nano(val: str) -> int:
    val = val.strip()
    if val.endswith("n"):
        return int(val[:-1])
    if val.endswith("m"):
        return int(float(val[:-1]) * 1_000_000)
    return int(float(val) * 1_000_000_000)


def _mem_to_bytes(val: str) -> int:
    val = val.strip()
    multipliers = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4}
    for suffix, mult in sorted(multipliers.items(), key=lambda x: -len(x[0])):
        if val.endswith(suffix):
            return int(float(val[: -len(suffix)]) * mult)
    return int(val)


if __name__ == "__main__":
    import json
    metrics = collect_node_metrics_api()
    print(json.dumps(metrics, indent=2))
