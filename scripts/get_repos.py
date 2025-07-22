import os
import gzip
import json
import re
import ssl
import unicodedata
import time
from collections import defaultdict
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
from functools import partial
from multiprocessing import Pool, cpu_count

ssl._create_default_https_context = ssl._create_unverified_context

# # CLI arguments
parser = argparse.ArgumentParser(description="Process GH Archive event counts and language stats.")
parser.add_argument("--year", type=int, required=True)
parser.add_argument("--month", type=int, required=True)
# parser.add_argument("--day", type=int, required=True)
args = parser.parse_args()

year = args.year
month = args.month
download_dir = f"data/gharchive_{year}_{month:02d}"
# day_str = f"{year}-{month:02d}-{day:02d}"

def is_english(text):
    if not text:
        return True
    for char in text:
        if char.isspace() or not char.isalpha():
            continue
        if not ('a' <= char.lower() <= 'z'):
            return False
    return True

event_counts = defaultdict(int)
english = 0
not_english = 0
all_languages = []
non_english_id_list = []

def process_hour(hour, day):
    day_str = f"{year}-{month:02d}-{day:02d}"
    filename = f"{day_str}-{hour}.json.gz"
    filepath = os.path.join(download_dir, filename)
    count = 0
    event_types = []
    repos_ids = []
    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    event = json.loads(line)
                    event_type = event.get("type")

                    if event_type =="PushEvent":
                        if 'bot' not in event.get("actor", {}).get("login", ""):
                            id = event.get('repo', {}).get('id', '')
                            name = event.get('repo', {}).get('name', '')
                            if id not in repos_ids:
                                ret_obj = {
                                    "id": id,
                                    "name": name
                                }
                                event_types.append(ret_obj)
                                repos_ids.append(id)

                except Exception as e:
                    continue
                    # raise e  # re-raise to handle in the main process
    except Exception as e:
        # raise e
        return None  # skip file if missing or error
    return event_types

if __name__ == "__main__":
    all_repos = []
    for day in range(1, 32):
        day_str = f"{year}-{month:02d}-{day:02d}"
        try:
            local_counts = defaultdict(int)
            all_detected_messages = []  # Store (language, message)
            start_time = time.time()
            func = partial(process_hour, day=day)
            with Pool(processes=min(cpu_count(), 24)) as pool:
                for result in tqdm(pool.imap_unordered(func, range(24)), total=24, desc="Processing hours"):
                    if result:
                        all_repos.extend(result)

        except Exception as e:
            print(f"Error processing day {day_str}: {e}")
            continue
    
    print(len(all_repos))
    df = pd.DataFrame(all_repos)
    df = df.drop_duplicates().reset_index(drop=True)
    df.to_csv(f"repos_{year}_{month:02d}.csv", index=False, encoding="utf-8")