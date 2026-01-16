import json
import pandas as pd

def dataLoading(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.json_normalize(data["messages"])

    df = df[
        (df["type"] == "message") &
        (df["text"].apply(lambda x: isinstance(x, str))) &
        (~df["text"].fillna("").astype(str).str.startswith("https://")) &
        (df["text"].fillna("").astype(str).str.strip() != "")
    ]


    blacklist = {'false', 'true', 'owner', 'ownership', 'company', 'gmbh', 'mvz', 'entity', 'practice', 'child', 'augenzentrum', 'n'}

    def clear(text):
        return any(x in text for x in blacklist)

    df = df[~df["text"].apply(clear)]
    df = df[["from", "text"]].rename(columns={"text": "message"})
    df["from"] = df["from"].apply(lambda x: "Витя" if x is None else x)

    df["from"] = df["from"].apply(lambda x: "Витя" if x == "Витя" else x)
    df["from"] = df["from"].apply(lambda x: "Егор" if x == "липтонзеленыйчай" else x)
    df["from"] = df["from"].apply(lambda x: "Саня" if x == "Санёк" else x)
    df["from"] = df["from"].apply(lambda x: "Ваня" if x == "Amd3d" else x)
    df["from"] = df["from"].apply(lambda x: "Влад" if x == "+7 916 436-44-86" else x)
    df["from"] = df["from"].apply(lambda x: "Артем" if x == "Артём" else x)
    df["from"] = df["from"].apply(lambda x: "Максим" if x == "Максим Пожидаев" else x)
    df["from"] = df["from"].apply(lambda x: "Костя" if x == "Константин Ефимов" else x)


    print(df.sample(20))

    for nickname, group in df.groupby("from"):
        with open(f"data/{nickname}.txt", "w", encoding="utf-8") as f:
            for msg in group["message"]:
                f.write(msg.strip() + "\n")