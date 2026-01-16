#!/usr/bin/python3

import sys, json


with open('./mkt.txt') as arquivo:
    lines = arquivo.readlines()

with open("../../data/file.json", "r+") as result:
    d = json.load(result)


for ip in lines:
    ip = ip.rstrip("\n")

    d[ip]['memoria_usada'] = []
    d[ip]['usuarios'] = []
    d[ip]["Lusuarios"] = []

    
d["label1"] = []
print(d)

with open("../../data/file.json", "w+") as result:
    json.dump(d, result, indent=4)

print("dados apagados.")