import json
import logging
import os
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import psutil
import redis
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.util import uuid_from_time
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("metric-worker")

NODE_ID = "1"
REDIS_HOST = os.getenv("REDIS_HOST", "192.168.0.5")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
SCYLLA_HOSTS = os.getenv(
    "SCYLLA_HOSTS", "192.168.0.5,192.168.0.6,192.168.0.7"
).split(",")
SCYLLA_KEYSPACE = os.getenv("SCYLLA_KEYSPACE", "my_keyspace")
SCYLLA_DC = os.getenv("SCYLLA_DC", "datacenter1")
COLLECT_INTERVAL = 1
WINDOW_SIZE = 5


class TransientError(Exception):
    """Lỗi tạm thời."""


@contextmanager
def get_redis():
    r = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True, socket_timeout=3
    )
    try:
        yield r
    except redis.ConnectionError as exc:
        logger.warning("Dragonfly unavailable: %s", exc)
        yield None
    finally:
        try:
            r.close()
        except Exception:
            pass


@retry(
    retry=retry_if_exception_type(TransientError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(10),
)
@contextmanager
def get_scylla():
    # SỬA 1: Thêm load_balancing_policy
    cluster = Cluster(
        contact_points=SCYLLA_HOSTS,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=SCYLLA_DC),
        protocol_version=4,
        connect_timeout=10,
    )
    session = None
    try:
        session = cluster.connect(SCYLLA_KEYSPACE)
        yield session
    except Exception as exc:
        if any(
            kw in str(exc) for kw in ("Connection refused", "Timeout", "Unavailable")
        ):
            raise TransientError(str(exc)) from exc
        raise
    finally:
        if session:
            try:
                session.shutdown()
            except Exception:
                pass
        try:
            cluster.shutdown()
        except Exception:
            pass


def get_google_latency():
    try:
        result = subprocess.run(
            ["ping", "-c", "1", "-W", "2", "google.com"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        for line in result.stdout.splitlines():
            if "time=" in line:
                return int(float(line.split("time=")[1].split(" ")[0]))
        return 0
    except Exception:
        return 0


def wait_until_ready(timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with get_redis() as r:
            redis_ok = r is not None and r.ping()
        try:
            with get_scylla() as session:
                session.execute("SELECT now() FROM system.local")
                scylla_ok = True
        except TransientError:
            scylla_ok = False
        if redis_ok and scylla_ok:
            logger.info("All backends ready.")
            return
        time.sleep(5)
    raise RuntimeError("Backends not ready")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS MetricData (
            id              timeuuid,
            node_id         text,
            "CPUUsageMax"     double,
            "CPUUsageMin"     double,
            "CPUUsageAVG"     double,
            "MemoryUsageMax"  double,
            "MemoryUsageMin"  double,
            "MemoryUsageAVG"  double,
            "TailLatencyMax"  double,
            "TailLatencyMin"  double,
            "TailLatencyAVG"  double,
            "DiskFreeMax"     double,
            "DiskFreeMin"     double,
            "DiskFreeAVG"     double,
            "SwapMemory_max"  double,
            "SwapMemory_min"  double,
            "SwapMemory_avg"  double,
            "DiskIOCounterBusyTimeMax"  double,
            "DiskIOCounterBusyTimeMin"  double,
            "DiskIOCounterBusyTimeAVG"  double,
            "NetIOCounterDropinMax"     double,
            "NetIOCounterDropinMin"     double,
            "NetIOCounterDropinAVG"     double,
            "NetIOCounterDropoutMax"    double,
            "NetIOCounterDropoutMin"    double,
            "NetIOCounterDropoutAVG"    double,
            PRIMARY KEY ((node_id), id)
        ) WITH CLUSTERING ORDER BY (id DESC)
    """)
    # session.execute("""
    #     DROP TABLE MetricData1; 
    # """)


    # # Đã sửa trường timestamp thành kiểu timestamp hợp lệ
    # session.execute("""
    #     CREATE TABLE IF NOT EXISTS MetricData1 (
    #         id              timeuuid,
    #         node_id         text,
    #         timestamp       timestamp,
    #         "CPUUsageMax"     double,
    #         "CPUUsageMin"     double,
    #         "CPUUsageAVG"     double,
    #         "MemoryUsageMax"  double,
    #         "MemoryUsageMin"  double,
    #         "MemoryUsageAVG"  double,
    #         "TailLatencyMax"  double,
    #         "TailLatencyMin"  double,
    #         "TailLatencyAVG"  double,
    #         "DiskFreeMax"     double,
    #         "DiskFreeMin"     double,
    #         "DiskFreeAVG"     double,
    #         "SwapMemory_max"  double,
    #         "SwapMemory_min"  double,
    #         "SwapMemory_avg"  double,
    #         "DiskIOCounterBusyTimeMax"  double,
    #         "DiskIOCounterBusyTimeMin"  double,
    #         "DiskIOCounterBusyTimeAVG"  double,
    #         "NetIOCounterDropinMax"     double,
    #         "NetIOCounterDropinMin"     double,
    #         "NetIOCounterDropinAVG"     double,
    #         "NetIOCounterDropoutMax"    double,
    #         "NetIOCounterDropoutMin"    double,
    #         "NetIOCounterDropoutAVG"    double,
    #         PRIMARY KEY ((node_id), id)
    #     ) WITH CLUSTERING ORDER BY (id DESC)
    # """)


def delete_old_data(session, node_id):
    rows = session.execute("SELECT id FROM MetricData WHERE node_id = %s", [node_id])
    for row in rows:
        session.execute(
            "DELETE FROM MetricData WHERE node_id = %s AND id = %s", [node_id, row.id]
        )


def insert_data(session, agg):
    # SỬA 2: Dùng datetime.now(timezone.utc) thay utcnow()
    now = datetime.now(timezone.utc)
    metric_id = uuid_from_time(now)

    # SỬA 3: Dùng đúng key từ agg dict
    session.execute(
        """INSERT INTO MetricData (
            id, node_id,
            "CPUUsageMax", "CPUUsageMin", "CPUUsageAVG",
            "MemoryUsageMax", "MemoryUsageMin", "MemoryUsageAVG",
            "TailLatencyMax", "TailLatencyMin", "TailLatencyAVG",
            "DiskFreeMax", "DiskFreeMin", "DiskFreeAVG",
            "SwapMemory_max", "SwapMemory_min", "SwapMemory_avg",
            "DiskIOCounterBusyTimeMax", "DiskIOCounterBusyTimeMin", "DiskIOCounterBusyTimeAVG",
            "NetIOCounterDropinMax", "NetIOCounterDropinMin", "NetIOCounterDropinAVG",
            "NetIOCounterDropoutMax", "NetIOCounterDropoutMin", "NetIOCounterDropoutAVG"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        [
            metric_id,
            agg["NODE_ID"],
            agg["CPUUsageMax"],
            agg["CPUUsageMin"],
            agg["CPUUsageAVG"],
            agg["MemoryUsageMax"],
            agg["MemoryUsageMin"],
            agg["MemoryUsageAVG"],
            agg["TailLatencyMax"],
            agg["TailLatencyMin"],
            agg["TailLatencyAVG"],
            agg["DiskFreeMax"],
            agg["DiskFreeMin"],
            agg["DiskFreeAVG"],
            agg["SwapMemoryMax"],
            agg["SwapMemoryMin"],
            agg["SwapMemoryAVG"],
            agg["DiskIOCounterBusyTimeMax"],
            agg["DiskIOCounterBusyTimeMin"],
            agg["DiskIOCounterBusyTimeAVG"],
            agg["NetIOCounterDropinMax"],
            agg["NetIOCounterDropinMin"],
            agg["NetIOCounterDropinAVG"],
            agg["NetIOCounterDropoutMax"],
            agg["NetIOCounterDropoutMin"],
            agg["NetIOCounterDropoutAVG"],
        ],
    )

    # # Đã bổ sung timestamp vào câu lệnh INSERT và truyền tham số `now`
    # session.execute(
    #     """INSERT INTO MetricData1 (
    #         id, node_id, timestamp,
    #         "CPUUsageMax", "CPUUsageMin", "CPUUsageAVG",
    #         "MemoryUsageMax", "MemoryUsageMin", "MemoryUsageAVG",
    #         "TailLatencyMax", "TailLatencyMin", "TailLatencyAVG",
    #         "DiskFreeMax", "DiskFreeMin", "DiskFreeAVG",
    #         "SwapMemory_max", "SwapMemory_min", "SwapMemory_avg",
    #         "DiskIOCounterBusyTimeMax", "DiskIOCounterBusyTimeMin", "DiskIOCounterBusyTimeAVG",
    #         "NetIOCounterDropinMax", "NetIOCounterDropinMin", "NetIOCounterDropinAVG",
    #         "NetIOCounterDropoutMax", "NetIOCounterDropoutMin", "NetIOCounterDropoutAVG"
    #     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
    #     [
    #         metric_id,
    #         agg["NODE_ID"],
    #         now,
    #         agg["CPUUsageMax"],
    #         agg["CPUUsageMin"],
    #         agg["CPUUsageAVG"],
    #         agg["MemoryUsageMax"],
    #         agg["MemoryUsageMin"],
    #         agg["MemoryUsageAVG"],
    #         agg["TailLatencyMax"],
    #         agg["TailLatencyMin"],
    #         agg["TailLatencyAVG"],
    #         agg["DiskFreeMax"],
    #         agg["DiskFreeMin"],
    #         agg["DiskFreeAVG"],
    #         agg["SwapMemoryMax"],
    #         agg["SwapMemoryMin"],
    #         agg["SwapMemoryAVG"],
    #         agg["DiskIOCounterBusyTimeMax"],
    #         agg["DiskIOCounterBusyTimeMin"],
    #         agg["DiskIOCounterBusyTimeAVG"],
    #         agg["NetIOCounterDropinMax"],
    #         agg["NetIOCounterDropinMin"],
    #         agg["NetIOCounterDropinAVG"],
    #         agg["NetIOCounterDropoutMax"],
    #         agg["NetIOCounterDropoutMin"],
    #         agg["NetIOCounterDropoutAVG"],
    #     ],
    # )

    logger.info("Insert OK node=%s", agg["NODE_ID"])


def main():
    if NODE_ID is None:
        logger.error("NODE_ID env missing.")
        raise SystemExit(1)

    wait_until_ready(timeout=120)

    with get_scylla() as session:
        create_table(session)

    data = []
    window_size = 0

    while True:
        try:
            if window_size != WINDOW_SIZE:
                cpu_times = psutil.cpu_times_percent()._asdict()
                net_io = psutil.net_io_counters()._asdict()
                disk_usage = psutil.disk_usage("/")._asdict()
                disk_io = psutil.disk_io_counters()._asdict()
                vm = psutil.virtual_memory()._asdict()
                sm = psutil.swap_memory()._asdict()
                time.sleep(COLLECT_INTERVAL)

                metric_data = {
                    "CPUUsage": 100 - cpu_times["idle"],
                    "MemoryUsage": (vm["total"] - vm["available"]) / vm["total"] * 100,
                    "TailLatency": get_google_latency(),
                    "DiskFree": disk_usage["free"] // (1024 * 1024),
                    "SwapMemory": sm["free"] // (1024 * 1024),
                    "DiskIOCounterBusyTime": disk_io["busy_time"],
                    "NetIOCounterDropin": net_io["dropin"],
                    "NetIOCounterDropout": net_io["dropout"],
                }
                data.append(metric_data)
                window_size += 1
            else:
                agg = {
                    "NODE_ID": NODE_ID,
                    **{
                        f"{k}{s}": (
                            max(node[k] for node in data)
                            if s == "Max"
                            else min(node[k] for node in data)
                            if s == "Min"
                            else sum(node[k] for node in data) / len(data)
                        )
                        for k in data[0]
                        for s in ["Max", "Min", "AVG"]
                    },
                }

                stored = False
                for attempt in range(5):
                    try:
                        with get_scylla() as session:
                            delete_old_data(session, NODE_ID)
                            insert_data(session, agg)
                            stored = True
                            break
                    except TransientError as exc:
                        logger.warning(
                            "ScyllaDB transient (attempt %d): %s", attempt + 1, exc
                        )
                        time.sleep(2**attempt)
                    except Exception as exc:
                        logger.exception("ScyllaDB error: %s", exc)
                        break

                if not stored:
                    with get_redis() as r:
                        if r:
                            r.set(f"metric:{NODE_ID}", json.dumps(agg), ex=60)

                data.clear()
                window_size = 0

        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
