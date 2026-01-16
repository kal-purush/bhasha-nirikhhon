# -*- coding: utf-8 -*-

import csv

arquivo = open ("brasil.csv")

soma = 0

for registro in csv.DictReader(arquivo):
    if registro.get("estado") == "":
        soma = soma + int(registro.get("habitantes"))
        #print (registro.get("habitantes"), registro.get("municipio"))

print soma

arquivo.close ()