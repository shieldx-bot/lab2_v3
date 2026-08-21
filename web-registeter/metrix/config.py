import os
from pathlib import Path

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

KUBECTL_BIN = os.getenv("KUBECTL_BIN", "kubectl")
KUBECONFIG = os.getenv("KUBECONFIG", "")

COLLECT_INTERVAL_SEC = int(os.getenv("COLLECT_INTERVAL_SEC", "30"))

CSV_HEADERS = [
    "timestamp",
    "node_name",
    "cpu_cores_requested",
    "cpu_percent_requested",
    "cpu_cores_allocatable",
    "mem_bytes_requested",
    "mem_percent_requested",
    "mem_bytes_allocatable",
]
