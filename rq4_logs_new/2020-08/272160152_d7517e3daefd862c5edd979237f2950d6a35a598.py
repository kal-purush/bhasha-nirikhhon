import http.client
from time import sleep
import random
for i in range(0,255):
    conn = http.client.HTTPConnection("192.168.0.15")
    print(i)
    red = str(hex(i))[2:]
    print(red)
    if i < 16:
        red = "0" + red
    print(red)
    strrequest = "/sec/?pt=35&ws={}0000".format(red)
    print(strrequest)
    conn.request("GET", strrequest)
    sleep(0.005)





    