import psutil
from os import listdir
from time import strftime, gmtime, time

def get_sys_info() -> dict:
    mem = psutil.virtual_memory().percent
    cpu = psutil.cpu_percent()
    swap = psutil.swap_memory().percent
    uptime = time() - psutil.boot_time()

    return {
        "cpu": cpu,
        "mem": mem,
        "swap": swap,
        "uptime": str(strftime("%H:%M:%S", gmtime(uptime)))
    }

def get_processes() -> list:
    processes_raw = listdir('/proc')
    pids = []
    processes = []

    for process in processes_raw:
        if(process.isdigit()):
            pids.append(process)

    for pid in pids:
        try:
            with open(f'/proc/{pid}/status') as pid_info:
                data = pid_info.readlines()
                name = data[0].split(":")[1]
                mem_usage = data[18].split(":")[1]
                processes.append({
                    "name": name.replace("\t", "").replace("\n", ""),
                    "pid": pid.replace("\t", "").replace("\n", ""),
                    "mem": mem_usage.replace("\t", "").replace("\n", "")
                })
        
        except FileNotFoundError:
            pass
            

    return processes