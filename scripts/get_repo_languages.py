import os
import gzip
import json
import re
import ssl
import unicodedata
from collections import defaultdict
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
# from lingua import Language, LanguageDetectorBuilder

# # Setup language detector
# languages = [Language.ENGLISH, Language.SPANISH, Language.FRENCH, Language.GERMAN,
#              Language.CHINESE, Language.JAPANESE, Language.HINDI, Language.BENGALI,
#              Language.RUSSIAN, Language.PORTUGUESE, Language.ARABIC]
# detector = LanguageDetectorBuilder.from_languages(*languages).build()

# Disable SSL validation if needed
ssl._create_default_https_context = ssl._create_unverified_context

# CLI arguments
parser = argparse.ArgumentParser(description="Process GH Archive event counts and language stats.")
parser.add_argument("--year", type=int, required=True)
parser.add_argument("--month", type=int, required=True)
parser.add_argument("--day", type=int, required=True)
args = parser.parse_args()

year = args.year
month = args.month
day = args.day
download_dir = f"data/gharchive_{year}_{month:02d}"
day_str = f"{year}-{month:02d}-{day:02d}"

# relevant_event_types = {
#     "IssuesEvent",
#     "IssueCommentEvent"
# }

relevant_event_types = {
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent"
}
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

def process_hour(hour):
    filename = f"{day_str}-{hour}.json.gz"
    filepath = os.path.join(download_dir, filename)
    count = 0
    event_types = []
    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line in f:
                if "\"language\":" in line:
                    # print(line.strip())
                    match = re.search(r'"language"\s*:\s*"([^"]+)"', line)
                    if match:
                        # print(match.group(1))  # Output: Perl6
                        event = json.loads(line)
                        id = event.get('repo', {}).get('id', '')
                        name = event.get('repo', {}).get('name', '')
                        ret_obj = {
                            "id": id,
                            "name": name,
                            "language": match.group(1)
                        }
                        event_types.append(ret_obj)
                    # count+=1
                    # if count==10:
                    #     break
    except Exception:
        # raise ValueError(f"Error processing file {filepath}")
        pass
    return event_types

# Process 24 hours
tasks = []
with ThreadPoolExecutor() as executor:
    for hour in range(24):
        tasks.append(executor.submit(process_hour, hour))
    for future in tqdm(as_completed(tasks), total=len(tasks), desc=f"Day {day:02d}"):
        languages = future.result()
        all_languages.extend(languages)
        
# Save ID info
# with open(f"result_files/repo_langauges_{year}_{month:02d}_{day:02d}.json", "w") as f:
#     json.dump(all_languages, f, indent=4, ensure_ascii=False)

df = pd.DataFrame(all_languages)
# os.makedirs("result_files", exist_ok=True)
df.to_csv(f"result_files/repo_languages_{year}_{month:02d}_{day:02d}.csv", index=False, encoding="utf-8")
