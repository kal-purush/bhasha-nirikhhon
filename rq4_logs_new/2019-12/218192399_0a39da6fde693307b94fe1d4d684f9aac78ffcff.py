import json
import time
import datetime
import pandas as pd
import numpy
import os
import math
from collections import defaultdict

TIME_THRESHOLD = 604800
SECONDS_IN_DAY = 86400
CURRENT_UNIX_TIME = datetime.datetime.now().timestamp()


json_file_name = input("Input JSON file name: ")

with open(json_file_name, errors = "ignore") as json_file:

        wabuJSON = json.load(json_file)

print(wabuJSON["CHATS"].keys())


dailyChatCounts = defaultdict(lambda: 0)

for i in wabuJSON["CHATS"].keys():
    for j in wabuJSON["CHATS"][i].keys():
        day = math.floor(wabuJSON["CHATS"][i][j]["timestamp"])

        date = datetime.datetime.utcfromtimestamp(day).strftime('%Y-%m-%d')

        dailyChatCounts[date] = dailyChatCounts[date] + 1 

chatCounts = []
for i in dailyChatCounts.keys():
    chatCounts.append(dailyChatCounts[i])

df = pd.DataFrame(data = {"chat_counts": chatCounts}, index=dailyChatCounts.keys())
df = df.sort_index()
df.to_csv("chats_by_day.csv")
os.system("start EXCEL.EXE chats_by_day.csv")