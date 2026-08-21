import sys
import time
import json
import logging
from datetime import datetime

from config import COLLECT_INTERVAL_SEC
from collector import collect_metrics
from exporter import save_csv, save_json, append_csv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def collect_once() -> list[dict]:
    try:
        rows = collect_metrics()
        if rows:
            csv_path = save_csv(rows, "snapshot")
            json_path = save_json(rows, "snapshot")
            append_csv(rows)
            logger.info("Collected %d node(s). CSV=%s  JSON=%s", len(rows), csv_path, json_path)
            print(json.dumps(rows, indent=2))
        else:
            logger.warning("No node metrics returned.")
        return rows
    except Exception as exc:
        logger.error("Collection failed: %s", exc)
        return []


def collect_loop():
    logger.info("Starting collection loop (interval=%ds). Press Ctrl+C to stop.", COLLECT_INTERVAL_SEC)
    try:
        while True:
            collect_once()
            time.sleep(COLLECT_INTERVAL_SEC)
    except KeyboardInterrupt:
        logger.info("Stopped by user.")


if __name__ == "__main__":
    if "--once" in sys.argv:
        collect_once()
    else:
        collect_loop()
