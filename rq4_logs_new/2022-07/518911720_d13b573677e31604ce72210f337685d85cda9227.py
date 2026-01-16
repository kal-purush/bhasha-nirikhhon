import string
import re

destino = open("prefixes.txt", "a")

f = open("combined.csv", "r")

prefixes = []

for line in f:
    n = re.split('(\d+)',line)
    k = n[0]
    if k not in prefixes:
        prefixes.append(k)
        destino.write(k + "\n")
