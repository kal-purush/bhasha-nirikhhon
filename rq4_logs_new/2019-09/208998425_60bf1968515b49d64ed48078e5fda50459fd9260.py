#%%
import datetime

#%%
canceled = {
    "id":"",
    "periods":[1,2],
    "dayofweek":2,
    "date":"2019-10-5",
    "teachers":[""],
    "lecture":""
}
canceled["date"] = datetime.datetime.strptime(canceled["date"], "%Y-%m-%d")

#%%
lectures = {
        "lecture": "",
        "periods": [1,2],
        "dayofweek": 2,
        "teachers": "",
}

#%%
user = {
    "id":"",
    "lectures": [lectures],
    "notify":[]
}

#%%
for i in range(len(user["lectures"])):
    if (user["lectures"][i]["periods"] in canceled["periods"]) and (user["lectures"][i]["dayofweek"] == canceled["dayofweek"]) and (user["lectures"][i]["teachers"] in canceled["teachers"]):
        print("aiu")

#%%
import pandas as pd
a = pd.read_csv("RSReferCsv.csv", encoding="ANSI", names=[i for i in range(7)], dtype="object")
a = a.drop([i for i in range(4)], axis=0)
a = a.drop([i for i in range(26, a.index.max()+1)])
a.columns = a.iloc[0, :]
a.drop(4, axis=0)
a
index = []
time_table = pd.DataFrame(columns=[i for i in range(0, 7)], index=["{}限_科目名".format(i) for i in range(1, 8)])
time_table