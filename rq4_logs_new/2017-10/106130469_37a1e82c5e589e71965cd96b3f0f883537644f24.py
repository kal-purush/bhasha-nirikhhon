from CsvRead import csv_read
from CsvWrite import csv_write


fieldnames = "ID_WALLET,Name,Balance".split(",")
values =  "58010707,Louis,1000.00".split(",")
myList = []
inner_dict = dict(zip(fieldnames, values))
myList.append(inner_dict)

csv_write(fieldnames,myList)
csv_read()