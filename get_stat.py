import os
import gzip
import json
import ssl
from collections import defaultdict
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import argparse
from lingua import Language, LanguageDetectorBuilder
from functools import partial
import regex as re 
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

def clean_body_text(text):
    if not text:
        return ""

    # Remove URLs
    text = re.sub(r'https?://\S+|www\.\S+', '', text)

    # Remove markdown/code blocks: inline and fenced
    text = re.sub(r'`{1,3}[^`]+`{1,3}', '', text)  # inline and code block
    text = re.sub(r'```[\s\S]+?```', '', text)     # fenced code block multiline

    # Remove emojis and other symbols
    text = re.sub(r'[^\p{L}\p{N}\s.,!?\'\"-]', '', text)

    return text.strip()

def is_english(text):
    if not text:
        return True
    for char in text:
        if char.isspace() or not char.isalpha():
            continue
        if not ('a' <= char.lower() <= 'z'):
            return False
    return True

def process_hour_mp(hour, year, month, day, download_dir):
    day_str = f"{year}-{month:02d}-{day:02d}"
    filename = f"{day_str}-{hour}.json.gz"
    filepath = os.path.join(download_dir, filename)
    local_counts = defaultdict(int)
    english_local = 0
    not_english_local = 0
    english_ids = []
    non_english_ids = []

    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    event = json.loads(line)
                    event_type = event.get("type")
                    # if event_type:
                        # local_counts[event_type] += 1

                    if event_type in {
                        "IssuesEvent", "IssueCommentEvent",
                        "PullRequestEvent", "PullRequestReviewEvent", "PullRequestReviewCommentEvent"
                    }:
                        if 'bot' not in event.get("actor", {}).get("login", ""):
                            payload = event.get("payload", {})
                            title = ""
                            body = ""
                            item_id = None
                            if event_type == "IssuesEvent":
                                if "issue" in payload:
                                    issue = payload.get("issue", {})
                                    title = issue.get("title", "")
                                    body = issue.get("body", "")
                                    item_id = issue.get("id", None)
                            
                            elif event_type == "IssueCommentEvent":
                                issue = payload.get("comment", {})
                                title = issue.get("title", "")
                                body = issue.get("body", "")
                                item_id = issue.get("id", None)
                            
                            elif event_type == "PullRequestReviewCommentEvent":
                                pr = payload.get("comment", {})
                                title = pr.get("title", "")
                                body = pr.get("body", "")
                                item_id = pr.get("id", None)
                            
                            elif event_type == "PullRequestReviewEvent":
                                pr = payload.get("review", {})
                                title = pr.get("title", "")
                                body = pr.get("body", "")
                                item_id = pr.get("id", None)

                            elif event_type == "PullRequestEvent":
                                pr = payload.get("pull_request", {})
                                title = pr.get("title", "")
                                body = pr.get("body", "")
                                item_id = pr.get("id", None)

                            if item_id:
                                cleaned_body = clean_body_text(body)
                                if is_english(title) and is_english(cleaned_body):
                                    english_local += 1
                                    english_ids.append({"id": item_id})
                                else:
                                    not_english_local += 1
                                    non_english_ids.append({"id": item_id})

                except Exception as e:
                    continue
                    # raise e  # re-raise to handle in the main process
    except Exception as e:
        # raise e
        return None  # skip file if missing or error

    return local_counts, english_local, not_english_local, english_ids, non_english_ids

if __name__ == "__main__":
    event_counts = defaultdict(int)
    english = 0
    not_english = 0
    english_id_list = []
    non_english_id_list = []

    with Pool(processes=cpu_count()) as pool:
        func = partial(process_hour_mp, year=year, month=month, day=day, download_dir=download_dir)
        for result in tqdm(pool.imap_unordered(func, range(24)), total=24, desc=f"Day {day:02d}"):
            if result is None:
                continue
            local_counts, eng, not_eng, eng_ids, not_eng_ids = result
            for k, v in local_counts.items():
                event_counts[k] += v
            english += eng
            not_english += not_eng
            english_id_list.extend(eng_ids)
            non_english_id_list.extend(not_eng_ids)

    # Save daily event counts
    event_counts['day'] = day
    df_all = pd.DataFrame([event_counts]).fillna(0).astype(int)
    # df_all.to_csv(f"gharchive_combined_counts_{year}_{month:02d}_{day:02d}.csv", index=False)

    # Save language stats
    df_english = pd.DataFrame([{
        "day": day,
        "english": english,
        "not_english": not_english
    }])
    df_english.to_csv(f"gharchive_combined_english_stats_{year}_{month:02d}_{day:02d}.csv", index=False)

    # Save ID lists as CSV
    pd.DataFrame(english_id_list).to_csv(
        f"combined_english_ids_{year}_{month:02d}_{day:02d}.csv", index=False)

    pd.DataFrame(non_english_id_list).to_csv(
        f"combined_non_english_ids_{year}_{month:02d}_{day:02d}.csv", index=False)
