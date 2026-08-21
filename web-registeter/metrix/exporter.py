import csv
import json
import os
from datetime import datetime
from pathlib import Path

from config import OUTPUT_DIR, CSV_HEADERS


def _ts_tag() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def save_csv(rows: list[dict], label: str = "nodes") -> Path:
    fname = f"{label}_{_ts_tag()}.csv"
    fpath = OUTPUT_DIR / fname
    fieldnames = list(rows[0].keys()) if rows else CSV_HEADERS
    with open(fpath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return fpath


def save_json(rows: list[dict], label: str = "nodes") -> Path:
    fname = f"{label}_{_ts_tag()}.json"
    fpath = OUTPUT_DIR / fname
    with open(fpath, "w") as f:
        json.dump(rows, f, indent=2)
    return fpath


def append_csv(rows: list[dict], filename: str = "node_metrics_history.csv") -> Path:
    fpath = OUTPUT_DIR / filename
    write_header = not fpath.exists() or os.path.getsize(fpath) == 0
    fieldnames = list(rows[0].keys()) if rows else CSV_HEADERS
    with open(fpath, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    return fpath
