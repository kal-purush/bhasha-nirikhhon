#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 10 11:26:31 2021

@author: root
"""

#!/usr/bin/env python3
from scapy.all import *
from scapy.layers.l2 import Ether
import scapy.all as scapy
import pickle
import numpy as np
import pandas as pd
import os
from datetime import datetime

arp = 2054
S1 = {}
data = {}
cantMuestras = 0

def mostrar_fuente(S):
    N = sum(S.values())
    simbolos = sorted(S.items(), key=lambda x: -x[1])
    print("S: ")
    print("\n".join([ " %s : %.5f - I: %.5f" % (d,k/N,- math.log(k/N,2)) for d,k in simbolos ]))
    H = - math.fsum([(k/N * math.log(k/N,2)) for d,k in simbolos ])
    print("H(S): %.5f" %  H )

def callback(pkt):
    if pkt.haslayer(scapy.ARP):
        global cantMuestras 
        arp = pkt[scapy.ARP]
        cantMuestras = cantMuestras + 1 
        print(cantMuestras)
        s_i = arp.psrc + " ("+str(arp.pdst)+")"
    
        if s_i not in S1:
            S1[s_i] = 0.0

        S1[s_i] += 1.0
        S2 = S1.copy()
        data[datetime.now()] = S2.values()

while cantMuestras < 100:
    
    sniff(prn=callback, count=100)

mostrar_fuente(S1)

df = pd.DataFrame.from_dict(data, orient='index', columns=S1.keys())
df = df.fillna(0)
df.to_csv('capturasS2.csv')