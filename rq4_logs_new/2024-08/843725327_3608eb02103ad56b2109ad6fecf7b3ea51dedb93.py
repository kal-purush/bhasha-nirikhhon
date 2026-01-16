#!/usr/bin/env python3

import time
import torch
from datetime import datetime

def deviceQuery(devid):
    s_time = datetime.now().strftime("%Y%m%d-%H%M%S")

    torch.cuda.device(devid)
    _id = torch.cuda.current_device()
    assert _id == devid

    device = torch.device("cuda:%d" % devid)

    props = torch.cuda.get_device_properties(device=device)
    #mem_used = torch.cuda.memory_reserved(device=device) / (1024 * 1024.) # I found a bug in pytorch
    mem_info = torch.cuda.mem_get_info(device=device)
    assert mem_info[1] == props.total_memory

    mem_total = mem_info[1] / (1024 * 1024.)
    mem_free =  mem_info[0] / (1024 * 1024.)
    mem_used = mem_total - mem_free

    print(f"{s_time}: [{devid} {props.name} {props.gcnArchName}] {mem_total:5.1f} {mem_used: 5.1f} {mem_free: 5.1f}", flush=True)


if __name__ == "__main__":

    ndev = torch.cuda.device_count()
    #print (f"ndev={ndev}")

    while True:
        for i in range(ndev):
            deviceQuery(i)
        time.sleep(1.0)
