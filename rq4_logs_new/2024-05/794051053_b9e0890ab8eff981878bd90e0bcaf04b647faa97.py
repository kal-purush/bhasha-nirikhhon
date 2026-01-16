import json
import hashlib
import logging
import os

from urllib.request import urlopen, Request

URL = "https://opravujeme.to/api/action/"
FILENAME = "data.json"

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    existing_ids, new_ids, removed_ids = set(), set(), set()
    existing_hashes = dict()
    if os.path.exists(FILENAME):
        with open(FILENAME) as f:
            existing_data = {j["id"]: j for j in json.load(f)}
            existing_ids = set(existing_data.keys())
        logging.info("Nahral jsem %d ids z existujiciho dumpu", len(existing_ids))

    qs = "?limit=100"
    data = []
    total = None
    while True:
        r = Request(URL + qs)
        logging.info("oteviram %s", r.full_url)
        with urlopen(r) as req:
            page = json.load(req)
            data.extend(page["objects"])
            if not total:
                total = page["meta"]["total_count"]

            if not page["meta"]["next"]:
                break
            qs = page["meta"]["next"]

    if len(data) != total:
        raise IOError(f"cekali jsme {total} elementu, mame {len(data)}")

    downloaded_data = {j["id"]: j for j in data}
    downloaded_ids = set(downloaded_data.keys())
    data.sort(key=lambda x: x["id"])

    new_ids = downloaded_ids - existing_ids
    removed_ids = existing_ids - downloaded_ids

    for el in data:
        el["sha1"] = hashlib.sha1(json.dumps(el).encode()).hexdigest()
        if el["id"] in existing_data and el["sha1"] != existing_data[el["id"]]["sha1"]:
            print("Zmeneno: {el['name']}")

    for new_id in sorted(new_ids):
        print(f"Pridano: {downloaded_data[new_id]['name']}")

    for removed_id in sorted(removed_ids):
        print(f"Odebrano: {existing_data[removed_id]['name']}")

    data.sort(key=lambda x: x["id"])
    logging.info("Ukladam %d elementu do %s", len(data), FILENAME)
    with open(FILENAME, "wt", encoding="utf-8") as fw:
        json.dump(data, fw, indent=2, sort_keys=True, ensure_ascii=False)