#!/usr/bin/python
# -*- coding: utf-8 -*-

from re import search
from subprocess import check_output

def getTime(acpi_in):
    TIME = search("\d\d:\d\d:\d\d", acpi_in)
    if TIME != None:
        return TIME.group(0)
    return TIME

def getStatusAndPercentage(acpi_in):
    return acpi_in.split(",",1)[0], int(search("(\d{1,3})%", acpi_in).group(1))


BATTERY_NUMBER = 0

ACPI = check_output(["acpi", "-b", "|", "grep", "'Battery " + str(BATTERY_NUMBER) + "'"]).split(": ",1)[1]
STATUS, PERCENTAGE = getStatusAndPercentage(ACPI)
TIME = getTime(ACPI)

SHORT_TEXT = ""

if PERCENTAGE <= 5:
    exit(33)
elif PERCENTAGE <= 25:
    SHORT_TEXT = "" # 1/4
elif PERCENTAGE <= 50:
    SHORT_TEXT = "" # 1/2
elif PERCENTAGE <= 75:
    SHORT_TEXT = "" # 3/4
else:
    SHORT_TEXT = "" # 1/1

SHORT_TEXT += " " + str(PERCENTAGE) + "%"
FULL_TEXT = SHORT_TEXT + (" - " + TIME[:-3] if TIME != None else "")

print(FULL_TEXT)
print(SHORT_TEXT)

if STATUS == "Charging":
    print("#A8FF00\n")
else:
    print("#FFAE00\n")

exit(0)