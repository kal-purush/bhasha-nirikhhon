import pandas as pd
import json

with open("../data/convert.json", "r", encoding="utf-8") as fcv:
    convert = json.load(fcv)

df = pd.read_csv("../data/GrahamUF.csv")

df.index = df["first/last"]
df = df.drop("first/last",axis=1)

#一度"BU"などの形にする
df_new = df.rename(columns=lambda s: s[3:5],
                 index=lambda s: s[3:5],
                 inplace=True)
#さらにひらがなにする
df_kana = df.rename(columns=lambda s: convert[s],
                    index=lambda s: convert[s],
                    inplace=True)
numbering_list = "あいうえかきくけさしすせたちつてなにぬねはひふへ"
algs = {}
print(df.loc["い"]["え"])
for x in numbering_list:
    for y in numbering_list:
        try:
            algs[x+y] = df.loc[x][y]
        except:
            pass

print(algs["はけ"])
print(algs["へな"])
with open("../data/myalgs.json", "w", encoding="utf-8") as fj:
    json.dump(algs, fj, ensure_ascii=False, indent=4)