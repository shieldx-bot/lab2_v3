import psutil
import time
import redis
import json
import random

 

from dotenv import load_dotenv
load_dotenv()
import os



NODE_ID = "2"
TIME_DELAY = 2
 
r = redis.Redis(
        host="192.168.24.2", port=6379, db=0, decode_responses=True)


while True:
    # snapshot đầu
    disk1 = psutil.disk_io_counters()
    net1 = psutil.net_io_counters()

    time.sleep((int(TIME_DELAY)))

    # snapshot sau
    disk2 = psutil.disk_io_counters()
    net2 = psutil.net_io_counters()

    cpu_usage = psutil.cpu_percent()
    memory_usage = psutil.virtual_memory().percent

    disk_io = (
        (disk2.read_bytes - disk1.read_bytes) +
        (disk2.write_bytes - disk1.write_bytes)
    ) / 1024 / 1024  # MB/s

    network_io = (
        (net2.bytes_recv - net1.bytes_recv) +
        (net2.bytes_sent - net1.bytes_sent)
    ) / 1024 / 1024  # MB/s

    metric_data = {
        "NODE_ID": int(NODE_ID),
        "CPU_Usage": round(cpu_usage, 2),
        "Memory_Usage": round(memory_usage, 2),
        "Disk_IO": round(disk_io, 2),
        "Network_IO": round(network_io, 2),
    }
    r.execute_command('JSON.SET', f"NODE-{NODE_ID}", '$', json.dumps(metric_data))

    print(metric_data)
