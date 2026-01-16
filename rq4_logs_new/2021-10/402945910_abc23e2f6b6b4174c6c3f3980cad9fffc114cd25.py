import pymongo
import pandas as pd


client = pymongo.MongoClient("mongodb://root:toor@localhost:27017/")
db = client["lost_in_paradise"]
col = db["lostItems"]

data = pd.read_excel("scripts/data.xlsx")
data = data.fillna("none")

insert = []

for index, row in data.iterrows():
    one = {
        "status": row["Status"],
        "what": {
            "category": row["Catergoria"],
            "description": row["Descripcion"],
        },
        "where": {
            "buldingName": row["Nombre_Edificio"],
            "roomNumber": row["Nombre_Espacio"],
            "recieved": row["DondeEntrego"],
        },
        "who": {
            "found": row["QuienEncontro"],
            "recieved": row["QuienRecibio"],
            "claimed": row["QuienReclamo"],
            "delivered": row["QuienEntrego"],
        },
        "when": {"recieved": row["FechaEntrada"], "delivered": row["FechaSalida"]},
        "observations": {
            "recieved": row["ObservacionesEntrada"],
            "delivered": row["ObservacionesSalida"],
        },
    }
    insert.append(one)

col.insert_many(insert)