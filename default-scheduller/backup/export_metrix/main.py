import psycopg2
import time
import psutil
import json
from datetime import datetime
import redis
from dotenv import load_dotenv
load_dotenv()
import os
NODE_ID = os.getenv("NODE_ID")
if NODE_ID == None: 
    print("I can't find varible environment")
    exit(0)
 



def main(): 
    data = [] 
    window_size = 0 
    r = redis.Redis(host='54.255.223.141', port=6379,  password='Vananh12345',  db=0, decode_responses=True)
    try:
        if r.ping():
            print("Connected to Redis!")
    except redis.ConnectionError:
        print("Could not connect to Redis.")

    connection = psycopg2.connect(host="54.255.223.141", database="postgres", user="postgres", password="Vananh12345", port=5432)
    cursor = connection.cursor()
    while True: 
        if window_size  != 5: 
            cpu_times = psutil.cpu_times_percent()._asdict()
            net_io_counters = psutil.net_io_counters()._asdict()
            disk_usage = psutil.disk_usage("/")._asdict()
            disk_io_counters = psutil.disk_io_counters()._asdict()
            virtual_memory = psutil.virtual_memory()._asdict()
            swap_memory = psutil.swap_memory()._asdict()
            print("+1")
            time.sleep(5)
            metric_data = {
          'CPUUsage': 100 - cpu_times['idle'],
          'MemoryUsage': (virtual_memory['total'] - virtual_memory['available']) / virtual_memory['total'],
          'TailLatency':    int(r.get("TailLatency-" + NODE_ID)) ,
          'DiskFree': disk_usage['free'] // (1024 * 1024) ,
          'SwapMemory': swap_memory['free'] // (1024 * 1024),
          'DiskIOCounterBusyTime': disk_io_counters['busy_time'],
          'NetIOCounterDropin': net_io_counters['dropin'],
          'NetIOCounterDropout': net_io_counters['dropout'] 
            }
            data.append(metric_data)
            window_size += 1 
            time.sleep(1)
        else: 
            
            metric_data = { 
                'NODE_ID': NODE_ID, 
                 'CPUUsageMax': max(node['CPUUsage'] for node in data), 
                 'CPUUsageMin': min(node['CPUUsage'] for node in data),
                 'CPUUsageAVG':  sum(node['CPUUsage'] for node in data) / len(data),
                 'MemoryUsageMax': max(node['MemoryUsage'] for node in data), 
                 'MemoryUsageMin': min(node['MemoryUsage'] for node in data), 
                 'MemoryUsageAVG': sum(node['MemoryUsage'] for node in data) / len(data),
                 'TailLatencyMax': max(node['TailLatency'] for node in data), 
                 'TailLatencyMin': min(node['TailLatency'] for node in data), 
                 'TailLatencyAVG': sum(node['TailLatency'] for node in data) / len(data),
                 'DiskFreeMax': max(node['DiskFree'] for node in data), 
                 'DiskFreeMin': min(node['DiskFree'] for node in data), 
                 'DiskFreeAVG': sum(node['DiskFree'] for node in data) / len(data),
                 'SwapMemory_max': max(node['SwapMemory'] for node in data), 
                 'SwapMemory_min': min(node['SwapMemory'] for node in data), 
                 'SwapMemory_avg': sum(node['SwapMemory'] for node in data) / len(data),
                 'DiskIOCounterBusyTimeMax': max(node['DiskIOCounterBusyTime'] for node in data), 
                 'DiskIOCounterBusyTimeMin': min(node['DiskIOCounterBusyTime'] for node in data), 
                 'DiskIOCounterBusyTimeAVG': sum(node['DiskIOCounterBusyTime'] for node in data) / len(data), 
                 'NetIOCounterDropinMax' : max(node['NetIOCounterDropin'] for node in data), 
                 'NetIOCounterDropinMin' : min(node['NetIOCounterDropin'] for node in data), 
                 'NetIOCounterDropinAVG' : sum(node['NetIOCounterDropin'] for node in data) / len(data),
                 'NetIOCounterDropoutMax' : max(node['NetIOCounterDropout'] for node in data), 
                 'NetIOCounterDropoutMin' : min(node['NetIOCounterDropout'] for node in data), 
                 'NetIOCounterDropoutAVG' : sum(node['NetIOCounterDropout'] for node in data) / len(data),
            }
            print(metric_data)
            data = []
            window_size  = 0 
            try: 
                cursor.execute("delete from MetricData where node_id = %s;", NODE_ID)
                insert_script = """INSERT INTO MetricData (NODE_ID, CPUUsageMax, CPUUsageMin, CPUUsageAVG, MemoryUsageMax, MemoryUsageMin, MemoryUsageAVG, TailLatencyMax, TailLatencyMin, TailLatencyAVG, DiskFreeMax, DiskFreeMin, DiskFreeAVG, SwapMemory_max, SwapMemory_min, SwapMemory_avg, DiskIOCounterBusyTimeMax, DiskIOCounterBusyTimeMin, DiskIOCounterBusyTimeAVG, NetIOCounterDropinMax, NetIOCounterDropinMin, NetIOCounterDropinAVG, NetIOCounterDropoutMax, NetIOCounterDropoutMin, NetIOCounterDropoutAVG) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                insert_value = (metric_data['NODE_ID'], metric_data['CPUUsageMax'], metric_data['CPUUsageMin'], metric_data['CPUUsageAVG'], metric_data['MemoryUsageMax'], metric_data['MemoryUsageMin'], metric_data['MemoryUsageAVG'], metric_data['TailLatencyMax'], metric_data['TailLatencyMin'], metric_data['TailLatencyAVG'], metric_data['DiskFreeMax'], metric_data['DiskFreeMin'], metric_data['DiskFreeAVG'], metric_data['SwapMemory_max'], metric_data['SwapMemory_min'], metric_data['SwapMemory_avg'], metric_data['DiskIOCounterBusyTimeMax'], metric_data['DiskIOCounterBusyTimeMin'], metric_data['DiskIOCounterBusyTimeAVG'], metric_data['NetIOCounterDropinMax'], metric_data['NetIOCounterDropinMin'], metric_data['NetIOCounterDropinAVG'], metric_data['NetIOCounterDropoutMax'], metric_data['NetIOCounterDropoutMin'], metric_data['NetIOCounterDropoutAVG'])
                cursor.execute(insert_script, insert_value)
                connection.commit()
                print("Đã chèn dữ liệu thành công!")
            except Exception as error:
                connection.rollback()  # Quan trọng: rollback trước khi thử lại
                print(f"Error When Insert Table: {error}")


            


 

 
if __name__ == '__main__': 
    main()