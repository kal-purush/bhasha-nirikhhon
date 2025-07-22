# import os
# import urllib.request
# import gzip
# import json
# import ssl
# from concurrent.futures import ThreadPoolExecutor

# # Configurable values
# YEAR = 2025
# MONTH = 5
# MAX_WORKERS = 8

# # Disable SSL certificate validation if needed
# ssl._create_default_https_context = ssl._create_unverified_context

# base_url = "https://data.gharchive.org"
# download_dir = f"gharchive_{YEAR}_{MONTH:02d}"
# os.makedirs(download_dir, exist_ok=True)

# relevant_event_types = {
#     "IssuesEvent",
#     "IssueCommentEvent",
#     "PullRequestEvent",
#     "PullRequestReviewEvent",
#     "PullRequestReviewCommentEvent"
# }

# def process_hour_file(day_str, hour):
#     filename = f"{day_str}-{hour}.json.gz"
#     url = f"{base_url}/{filename}"
#     filepath = os.path.join(download_dir, filename)
#     events = []

#     if not os.path.exists(filepath):
#         try:
#             urllib.request.urlretrieve(url, filepath)
#         except Exception as e:
#             print(f"Failed to download {filename}: {e}")
#             return []

#     # try:
#     #     with gzip.open(filepath, 'rt', encoding='utf-8') as f:
#     #         for line in f:
#     #             try:
#     #                 event = json.loads(line)
#     #                 if event.get("type") in relevant_event_types:
#     #                     events.append(event)
#     #             except json.JSONDecodeError:
#     #                 continue
#     # except Exception as e:
#     #     print(f"Failed to process {filename}: {e}")
#     #     return []

#     return events

# def process_day(day):
#     day_str = f"{YEAR}-{MONTH:02d}-{day:02d}"
#     print(f"Processing {day_str}...")

#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         results = list(executor.map(lambda h: process_hour_file(day_str, h), range(24)))

#     events = [event for hour_events in results for event in hour_events]

#     out_file = os.path.join(download_dir, f"filtered_{day_str}.json")
#     with open(out_file, "w", encoding="utf-8") as f:
#         json.dump(events, f, ensure_ascii=False)

#     print(f"Saved {len(events)} events to {out_file}")

# # Run for a range of days
# for day in range(1, 2):  # Change to (1, 32) for full month
#     process_day(day)

import os
import sys
import urllib.request
import ssl
from concurrent.futures import ThreadPoolExecutor

# Take year and month from command-line arguments
# if len(sys.argv) != 3:
#     print("Usage: python script.py <YEAR> <MONTH>")
#     sys.exit(1)

# YEAR = int(sys.argv[1])
# MONTH = int(sys.argv[2])
# YEAR = 2025
# MONTH = 5
# MAX_WORKERS = 8

# # Disable SSL certificate validation if needed
# ssl._create_default_https_context = ssl._create_unverified_context

# base_url = "https://data.gharchive.org"
# download_dir = f"gharchive_{YEAR}_{MONTH:02d}"
# os.makedirs(download_dir, exist_ok=True)

# def download_hour_file(day_str, hour):
#     filename = f"{day_str}-{hour}.json.gz"
#     url = f"{base_url}/{filename}"
#     filepath = os.path.join(download_dir, filename)

#     if os.path.exists(filepath):
#         print(f"Already exists: {filename}")
#         return

#     try:
#         urllib.request.urlretrieve(url, filepath)
#         print(f"Downloaded: {filename}")
#     except Exception as e:
#         print(f"Failed to download {filename}: {e}")

# def download_day(day):
#     day_str = f"{YEAR}-{MONTH:02d}-{day:02d}"
#     print(f"Downloading files for {day_str}...")

#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         executor.map(lambda h: download_hour_file(day_str, h), range(24))

# # Loop through the days of the month (assume max 31 days; GHArchive will 404 on invalid ones)
# for day in range(1, 32):
#     download_day(day)

# import os
# import sys
# import urllib.request
# import ssl
# from concurrent.futures import ThreadPoolExecutor
# from multiprocessing import Pool

# # Take year and month from command-line arguments
# if len(sys.argv) != 3:
#     print("Usage: python script.py <YEAR> <MONTH>")
#     sys.exit(1)

# YEAR = int(sys.argv[1])
# MONTH = int(sys.argv[2])
# MAX_WORKERS = 24

# # Disable SSL certificate validation if needed
# ssl._create_default_https_context = ssl._create_unverified_context

# base_url = "https://data.gharchive.org"
# download_dir = f"data/gharchive_{YEAR}_{MONTH:02d}"
# os.makedirs(download_dir, exist_ok=True)

# def download_hour_file(day_str, hour):
#     print(f"Downloading {day_str} hour {hour}...")
#     filename = f"{day_str}-{hour}.json.gz"
#     url = f"{base_url}/{filename}"
#     filepath = os.path.join(download_dir, filename)

#     if os.path.exists(filepath):
#         print(f"Already exists: {filename}")
#         return

#     import socket
#     try:
#         # Set a timeout (e.g., 60 seconds) for the initial connection
#         with urllib.request.urlopen(url, timeout=60) as response, open(filepath, 'wb') as out_file:
#             while True:
#                 try:
#                     chunk = response.read(8192)
#                     if not chunk:
#                         break
#                     out_file.write(chunk)
#                 except socket.timeout:
#                     print(f"Timeout while downloading {filename}. Aborting.")
#                     break
#         print(f"Downloaded: {filename}")
#     except Exception as e:
#         print(f"Failed to download {filename}: {e}")

# def download_day(day):
#     day_str = f"{YEAR}-{MONTH:02d}-{day:02d}"
#     print(f"Downloading files for {day_str}...")

#     with Pool(processes=MAX_WORKERS) as pool:
#         pool.starmap(download_hour_file, [(day_str, h) for h in range(24)])

# # Loop through the days of the month (assume max 31 days; GHArchive will 404 on invalid ones)
# for day in range(1, 32):
#     download_day(day)

# # download_day(3)  # Example for a specific day, change as needed

import os
import sys
import urllib.request
import ssl

if len(sys.argv) != 5:
    print("Usage: python script.py <YEAR> <MONTH> <DAY> <HOUR>")
    sys.exit(1)

YEAR = int(sys.argv[1])
MONTH = int(sys.argv[2])
DAY = int(sys.argv[3])
HOUR = int(sys.argv[4])

ssl._create_default_https_context = ssl._create_unverified_context

base_url = "https://data.gharchive.org"
download_dir = f"data/gharchive_{YEAR}_{MONTH:02d}"
os.makedirs(download_dir, exist_ok=True)

def download_hour_file(year, month, day, hour):
    day_str = f"{year}-{month:02d}-{day:02d}"
    filename = f"{day_str}-{hour}.json.gz"
    url = f"{base_url}/{filename}"
    filepath = os.path.join(download_dir, filename)

    if os.path.exists(filepath):
        print(f"Already exists: {filename}")
        return

    print(f"Downloading {day_str} hour {hour}...")

    import socket
    try:
        with urllib.request.urlopen(url, timeout=60) as response, open(filepath, 'wb') as out_file:
            while True:
                try:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    out_file.write(chunk)
                except socket.timeout:
                    print(f"Timeout while downloading {filename}. Aborting.")
                    break
        print(f"Downloaded: {filename}")
    except Exception as e:
        print(f"Failed to download {filename}: {e}")

download_hour_file(YEAR, MONTH, DAY, HOUR)