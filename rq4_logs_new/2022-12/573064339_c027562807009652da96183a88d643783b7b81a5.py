import requests
import datetime
import re
import os

sites = [
    "https://www.lipocorpus.com.br/",
    "https://www.revitabeauty.com.br/",
    "https://www.clearbeauty.com.br/"
    ]

def toLogPath(url):
    return re.sub(r'[^\w_. -]', '_', url) + ".log"

for site in sites:
    if not os.path.isfile(toLogPath(site)):
        with open(toLogPath(site), "w") as f:
            pass

for site in sites:
    status = ""
    try:
        resp = requests.get(site, timeout=10)
        resp.raise_for_status()
        if resp.ok:
            status = "Up"
    except:
        status = "Down"

    time = str(datetime.datetime.now())

    last_status = ""
    with open(toLogPath(site), "r") as f:
        for line in f:
            last_status = str(line.split(";")[-1][:-1])

    if status != last_status:
    
        new_line = time + ";" + site + ";" + status + "\n"

        with open(toLogPath(site), "a") as f:
            f.write(new_line)