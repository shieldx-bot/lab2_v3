import csv
import os
import logging
from datetime import datetime
from contextlib import contextmanager
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# --- Giữ nguyên cấu hình kết nối từ code của bạn ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("export-csv")

SCYLLA_HOSTS = os.getenv("SCYLLA_HOSTS", "192.168.24.2,192.168.24.3,192.168.24.4").split(",")
SCYLLA_KEYSPACE = os.getenv("SCYLLA_KEYSPACE", "my_keyspace")
SCYLLA_DC = os.getenv("SCYLLA_DC", "datacenter1")

class TransientError(Exception):
    """Lỗi tạm thời."""

@retry(
    retry=retry_if_exception_type(TransientError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(10),
)
@contextmanager
def get_scylla():
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
        if any(kw in str(exc) for kw in ("Connection refused", "Timeout", "Unavailable")):
            raise TransientError(str(exc)) from exc
        raise
    finally:
        if session:
            try: session.shutdown()
            except Exception: pass
        try: cluster.shutdown()
        except Exception: pass

# --- Hàm Export chính ---
def export_metric_data_1_to_csv(output_filename="metric_data_1_export.csv"):
    # Định nghĩa danh sách các cột chính xác theo schema của MetricData1
    # Lưu ý: Các cột có chữ viết hoa hỗn hợp cần bọc trong dấu nháy kép khi viết câu lệnh CQL
    fields = [
        "node_id", "id", "timestamp",
        "CPUUsageMax", "CPUUsageMin", "CPUUsageAVG",
        "MemoryUsageMax", "MemoryUsageMin", "MemoryUsageAVG",
        "TailLatencyMax", "TailLatencyMin", "TailLatencyAVG",
        "DiskFreeMax", "DiskFreeMin", "DiskFreeAVG",
        "SwapMemory_max", "SwapMemory_min", "SwapMemory_avg",
        "DiskIOCounterBusyTimeMax", "DiskIOCounterBusyTimeMin", "DiskIOCounterBusyTimeAVG",
        "NetIOCounterDropinMax", "NetIOCounterDropinMin", "NetIOCounterDropinAVG",
        "NetIOCounterDropoutMax", "NetIOCounterDropoutMin", "NetIOCounterDropoutAVG"
    ]
    
    # Xây dựng câu lệnh SELECT bằng cách bọc các cột có ký tự hoa vào cặp dấu ""
    cql_columns = [f'"{f}"' if any(c.isupper() for c in f) or "_" in f else f for f in fields]
    query = f"SELECT {', '.join(cql_columns)} FROM MetricData5"
    
    logger.info("Đang kết nối tới ScyllaDB để lấy dữ liệu...")
    
    try:
        with get_scylla() as session:
            # Thiết kế fetch_size để tránh tràn bộ nhớ nếu dữ liệu quá lớn (Paging)
            statement = session.prepare(query)
            statement.fetch_size = 5000 
            
            rows = session.execute(statement)
            
            logger.info("Bắt đầu ghi dữ liệu ra file: %s", output_filename)
            with open(output_filename, mode="w", newline="", encoding="utf-8") as csv_file:
                # Dùng DictWriter để map dữ liệu từ Row object sang CSV chính xác theo tên cột
                writer = csv.DictWriter(csv_file, fieldnames=fields)
                writer.writeheader()
                
                count = 0
                for row in rows:
                    # Chuyển đổi dữ liệu row thành dictionary thuần túy
                    row_dict = {}
                    for field in fields:
                        val = getattr(row, field, None)
                        # Format lại datetime nếu cần (hoặc để mặc định string của Python)
                        if isinstance(val, datetime):
                            val = val.isoformat()
                        row_dict[field] = val
                        
                    writer.writerow(row_dict)
                    count += 1
                    if count % 10000 == 0:
                        logger.info("Đã export được %d dòng...", count)
                        
            logger.info("Export hoàn tất thành công! Tổng số dòng: %d", count)
            
    except Exception as e:
        logger.error("Đã xảy ra lỗi trong quá trình export: %s", e)

if __name__ == "__main__":
    # Bạn có thể đổi tên file tùy ý tại đây
    filename = f"metric_data_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    export_metric_data_1_to_csv(filename)