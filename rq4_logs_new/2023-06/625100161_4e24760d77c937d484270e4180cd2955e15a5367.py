import requests as req
from time import sleep
node = "SOME_NODE_NAME"
url = "WHATEVER_IP_OR_URL:PORT"
baseurl = f"http://{url}/{node}"

def setpoint(T): return req.get(f"{baseurl}/setpoint/{T}").json()
def saveon():    return req.get(f"{baseurl}/save/on").json()
def saveoff():   return req.get(f"{baseurl}/save/off").json()
def experiment(exp): return req.get(f"{baseurl}/experiment/{exp}").json()
def setpid(kp,ki,kd): return req.get(f"{baseurl}/setpid/{kp}/{ki}/{kd}")

#set up new experiment
experiment("calib_test_X")
sleep(1)
saveon()
sleep(1)
for i in range(80,120):
    setpoint(i)
    sleep(60)

setpoint(0)
sleep(1800)
saveoff()