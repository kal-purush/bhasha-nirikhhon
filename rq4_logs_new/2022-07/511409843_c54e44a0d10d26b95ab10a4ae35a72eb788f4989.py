import csv
import glob
from typing import Set, Dict, Union

stations_file = "stations.csv"
trip_files = glob.glob("2021-*.csv")

trips_header = [
    "departure_time",
    "return_time",
    "departure_station",
    "return_time",
    "distance",
    "duration",
]

stations_header = [
    "fid",
    "id",
    "name_fi",
    "name_se",
    "name_en",
    "address_fi",
    "address_se",
    "city_fi",
    "city_se",
    "operator",
    "capacity",
    "x",
    "y",
]

stations_ids: Set[int] = set()
stations = []

# Read all stations and override the header
with open(stations_file, mode="r") as s:
    rows = csv.DictReader(s)

    for r in rows:
        stations_ids.add(r["ID"])
        stations.append({
            "fid": r["FID"],
            "id": r["ID"],
            "name_fi": r["Nimi"],
            "name_se": r["Namn"],
            "name_en": r["Name"],
            "address_fi": r["Osoite"],
            "address_se": r["Adress"],
            "city_fi": r["Kaupunki"],
            "city_se": r["Stad"],
            "operator": r["Operaattor"],
            "capacity": r["Kapasiteet"],
            "x": r["x"],
            "y": r["y"],
        })

with open(stations_file, mode="w") as s:
    writer = csv.DictWriter(s, fieldnames=stations_header)
    writer.writeheader()
    writer.writerows(stations)