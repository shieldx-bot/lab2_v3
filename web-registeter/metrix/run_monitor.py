import sys
import time
import json
import signal
import logging
from datetime import datetime

from config import COLLECT_INTERVAL_SEC
from collector import collect_metrics
from exporter import save_csv, save_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

_buffer: list[dict] = []


def _export_and_exit(signum, frame):
    tag = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = save_csv(_buffer, f"node_metrics_{tag}")
    logger.info("Exported %d records -> %s", len(_buffer), csv_path)
    print(f"\n[EXPORTED] {len(_buffer)} records saved to {csv_path}")
    sys.exit(0)


def collect_once() -> list[dict]:
    try:
        rows = collect_metrics()
        if rows:
            logger.info("Collected %d node(s)", len(rows))
        else:
            logger.warning("No node metrics returned.")
        return rows
    except Exception as exc:
        logger.error("Collection failed: %s", exc)
        return []


def collect_loop():
    signal.signal(signal.SIGINT, _export_and_exit)
    signal.signal(signal.SIGTERM, _export_and_exit)

    logger.info("Collecting every %ds. Press Ctrl+C to stop & export CSV.", COLLECT_INTERVAL_SEC)

    while True:
        rows = collect_once()
        if rows:
            _buffer.extend(rows)
        time.sleep(COLLECT_INTERVAL_SEC)


if __name__ == "__main__":
    if "--once" in sys.argv:
        rows = collect_once()
        if rows:
            csv_path = save_csv(rows, "snapshot")
            print(f"Saved {len(rows)} records -> {csv_path}")
    else:
        collect_loop()
