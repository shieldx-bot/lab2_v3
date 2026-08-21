import os
import time
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

try:
    import psutil
except ImportError:
    sys.exit("psutil not installed. Run: pip install psutil")

OUTPUT_DIR = Path(__file__).parent / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

COLLECT_INTERVAL_SEC = int(os.getenv("COLLECT_INTERVAL_SEC", "15"))


def collect_vm_metrics() -> dict:
    now = datetime.now(timezone.utc).isoformat()

    cpu_percent = psutil.cpu_percent(interval=1)
    cpu_count = psutil.cpu_count()
    cpu_freq = psutil.cpu_freq()
    load_avg = os.getloadavg()

    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    disk = psutil.disk_usage("/")

    return {
        "timestamp": now,
        "hostname": os.uname().nodename,
        "cpu_percent": cpu_percent,
        "cpu_count": cpu_count,
        "cpu_freq_mhz": round(cpu_freq.current, 2) if cpu_freq else None,
        "load_1m": round(load_avg[0], 2),
        "load_5m": round(load_avg[1], 2),
        "load_15m": round(load_avg[2], 2),
        "mem_total_bytes": mem.total,
        "mem_used_bytes": mem.used,
        "mem_available_bytes": mem.available,
        "mem_percent": mem.percent,
        "swap_total_bytes": swap.total,
        "swap_used_bytes": swap.used,
        "swap_percent": swap.percent,
        "disk_total_bytes": disk.total,
        "disk_used_bytes": disk.used,
        "disk_percent": disk.percent,
    }


def save_snapshot(data: dict) -> Path:
    tag = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fpath = OUTPUT_DIR / f"vm_metrics_{tag}.json"
    with open(fpath, "w") as f:
        json.dump(data, f, indent=2)
    return fpath


def _export_and_exit(signum, frame):
    tag = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fpath = OUTPUT_DIR / f"vm_metrics_{tag}.json"
    with open(fpath, "w") as f:
        json.dump(_buffer, f, indent=2)
    logger.info("Exported %d records -> %s", len(_buffer), fpath)
    print(f"\n[EXPORTED] {len(_buffer)} records saved to {fpath}")
    sys.exit(0)


_buffer: list[dict] = []


def collect_loop():
    signal.signal(signal.SIGINT, _export_and_exit)
    signal.signal(signal.SIGTERM, _export_and_exit)

    logger.info(
        "Collecting VM metrics every %ds. Press Ctrl+C to stop & export JSON.",
        COLLECT_INTERVAL_SEC,
    )

    while True:
        try:
            row = collect_vm_metrics()
            _buffer.append(row)
            logger.info(
                "CPU: %.1f%% | RAM: %.1f%% (%s/%s)",
                row["cpu_percent"],
                row["mem_percent"],
                _fmt_bytes(row["mem_used_bytes"]),
                _fmt_bytes(row["mem_total_bytes"]),
            )
        except Exception as exc:
            logger.error("Collection failed: %s", exc)

        time.sleep(COLLECT_INTERVAL_SEC)


def _fmt_bytes(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024.0:
            return f"{b:.1f} {unit}"
        b /= 1024.0
    return f"{b:.1f} PB"


if __name__ == "__main__":
    if "--once" in sys.argv:
        data = collect_vm_metrics()
        fpath = save_snapshot(data)
        print(json.dumps(data, indent=2))
        print(f"\nSaved -> {fpath}")
    else:
        collect_loop()
