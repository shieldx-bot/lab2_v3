import re
import subprocess
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from config import KUBECTL_BIN, KUBECONFIG

logger = logging.getLogger(__name__)


def _run_kubectl(args: list[str]) -> str:
    cmd = [KUBECTL_BIN] + args
    env = None
    if KUBECONFIG:
        import os
        env = {**os.environ, "KUBECONFIG": KUBECONFIG}
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"kubectl failed: {result.stderr.strip()}")
    return result.stdout


def parse_top_nodes(raw: str) -> list[dict]:
    """Parse 'kubectl top nodes' human-readable output."""
    lines = [l for l in raw.strip().splitlines() if l.strip()]
    if len(lines) < 2:
        return []

    rows = []
    for line in lines[1:]:
        parts = line.split()
        if len(parts) < 5:
            continue
        name = parts[0]
        cpu_val = parts[1]
        cpu_pct = parts[2]
        mem_val = parts[3]
        mem_pct = parts[4]
        rows.append({
            "node_name": name,
            "cpu_requested": cpu_val,
            "cpu_percent_requested": cpu_pct,
            "mem_requested": mem_val,
            "mem_percent_requested": mem_pct,
        })
    return rows


def _parse_cpu_to_cores(val: str) -> float:
    val = val.strip()
    if val.endswith("m"):
        return float(val[:-1]) / 1000.0
    if val.endswith("n"):
        return float(val[:-1]) / 1_000_000_000.0
    return float(val)


def _parse_mem_to_bytes(val: str) -> int:
    val = val.strip()
    multipliers = {
        "Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4,
        "K": 1000, "M": 1000**2, "G": 1000**3, "T": 1000**4,
    }
    for suffix, mult in sorted(multipliers.items(), key=lambda x: -len(x[0])):
        if val.endswith(suffix):
            return int(float(val[: -len(suffix)]) * mult)
    return int(val)


def parse_top_nodes_json(raw: str) -> list[dict]:
    """Parse 'kubectl top nodes -o json' output."""
    data = json.loads(raw)
    rows = []
    for item in data.get("items", []):
        meta = item.get("metadata", {})
        usage = item.get("usage", {})
        cap = item.get("capacity", {})
        rows.append({
            "node_name": meta.get("name", ""),
            "cpu_cores_used": _parse_cpu_to_cores(usage.get("cpu", "0")),
            "cpu_percent_used": "",
            "mem_bytes_used": _parse_mem_to_bytes(usage.get("memory", "0")),
            "mem_percent_used": "",
            "cpu_cores_allocatable": _parse_cpu_to_cores(cap.get("cpu", "0")),
            "mem_bytes_allocatable": _parse_mem_to_bytes(cap.get("memory", "0")),
        })
        if rows[-1]["cpu_cores_allocatable"] > 0:
            rows[-1]["cpu_percent_used"] = round(
                rows[-1]["cpu_cores_used"] / rows[-1]["cpu_cores_allocatable"] * 100, 2
            )
        if rows[-1]["mem_bytes_allocatable"] > 0:
            rows[-1]["mem_percent_used"] = round(
                rows[-1]["mem_bytes_used"] / rows[-1]["mem_bytes_allocatable"] * 100, 2
            )
    return rows


def _collect_raw_top() -> tuple[str, Optional[str]]:
    """Return (human output, json-or-error)."""
    human = _run_kubectl(["top", "nodes"])
    try:
        js = _run_kubectl(["top", "nodes", "-o", "json"])
    except Exception:
        js = None
    return human, js


def collect_metrics() -> list[dict]:
    """Collect node metrics and return normalized list of dicts."""
    now = datetime.now(timezone.utc).isoformat()
    human, js_raw = _collect_raw_top()

    if js_raw:
        try:
            rows = parse_top_nodes_json(js_raw)
        except (json.JSONDecodeError, KeyError):
            rows = parse_top_nodes(human)
    else:
        rows = parse_top_nodes(human)

    for r in rows:
        r["timestamp"] = now
    return rows


if __name__ == "__main__":
    import json
    metrics = collect_metrics()
    print(json.dumps(metrics, indent=2))
