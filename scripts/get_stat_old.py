import os
import gzip
import json
import ssl
import unicodedata
from collections import defaultdict
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import matplotlib.pyplot as plt

# Skip SSL certificate validation (use only if necessary)
ssl._create_default_https_context = ssl._create_unverified_context

# Parse command line arguments for year and month
parser = argparse.ArgumentParser(description="Process GH Archive event counts.")
parser.add_argument("--year", type=int, required=True, help="Year (e.g., 2025)")
parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
args = parser.parse_args()

year = args.year
month = args.month

download_dir = f"data/gharchive_{year}_{month:02d}"

def is_latin(text):
    if not text:
        return True
    for char in text:
        if char.isalpha():
            name = unicodedata.name(char, "")
            if "LATIN" not in name:
                return False
    return True

# Dictionary to hold counts: {day: {event_type: count}}
event_counts = defaultdict(lambda: defaultdict(int))

def process_file(day, hour):
    local_counts = defaultdict(int)
    filename = f"{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    filepath = os.path.join(download_dir, filename)
    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    event = json.loads(line)
                    event_type = event.get("type")
                    if event_type:
                        local_counts[event_type] += 1
                except json.JSONDecodeError:
                    continue
    except Exception:
        pass
    return day, local_counts

tasks = []
with ThreadPoolExecutor() as executor:
    for day in range(1, 32):
        for hour in range(24):
            tasks.append(executor.submit(process_file, day, hour))
    for future in tqdm(as_completed(tasks), total=len(tasks), desc="Processing files"):
        day, local_counts = future.result()
        for event_type, count in local_counts.items():
            event_counts[day][event_type] += count

# Convert to DataFrame for plotting
df_counts = pd.DataFrame(event_counts).fillna(0).astype(int).T.sort_index()

# Save DataFrame to CSV
csv_filename = f"gharchive_event_counts_{year}_{month:02d}.csv"
df_counts.to_csv(csv_filename)