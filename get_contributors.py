import argparse
import os
import gzip
import json
from collections import defaultdict
import time
import pandas as pd
import regex as re
import unicodedata
from multiprocessing import Pool, cpu_count
from functools import partial
from tqdm import tqdm

parser = argparse.ArgumentParser(description="Process GH Archive event counts and language stats.")
parser.add_argument("--year", type=int, required=True)
parser.add_argument("--month", type=int, required=True)
args = parser.parse_args()

year = args.year
month = args.month


def recursive_defaultdict_to_dict(d):
    if isinstance(d, defaultdict):
        d = {k: recursive_defaultdict_to_dict(v) for k, v in d.items()}
    elif isinstance(d, list):
        d = [recursive_defaultdict_to_dict(i) for i in d]
    return d

def process_hour(hour, year, month, day, download_dir, repo_ids):
    day_str = f"{year}-{month:02d}-{day:02d}"
    filename = f"{day_str}-{hour}.json.gz"
    filepath = os.path.join(download_dir, filename)
    contributors = defaultdict(set)

    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    event = json.loads(line)
                    event_type = event.get("type")
                    if event_type in { "PushEvent", "PullRequestEvent", "PullRequestReviewCommentEvent", "IssuesEvent", "IssueCommentEvent",
                                      "CommitCommentEvent", "CreateEvent", "ReleaseEvent"}:
                        actor = event.get("actor", {}).get("login", "")
                        if actor and 'bot' not in actor:
                            repo_id = event.get("repo", {}).get("id", None)
                            if repo_id in repo_ids:
                                contributors[repo_id].add(actor)
                except Exception:
                    pass
    except Exception:
        return None
    return contributors

if __name__ == "__main__":
    ## create interactions_files directory if it doesn't exist
    if not os.path.exists("contributors_files"):
        os.makedirs("contributors_files")
    all_comment_count = defaultdict(lambda: defaultdict(lambda: {"repo_id": None, "html_url": None, "comment_count": 0}))
    all_contributors = defaultdict(set)
    all_issue_close_durations = defaultdict(list)
    
    df = pd.read_csv('repo_language_classification1.csv')
    repo_ids = set(df['repo_id'].tolist())

    for day in range(1, 32):  # Change to range(1, 33) for all days in month
        try:
            download_dir = f"data/gharchive_{year}_{month:02d}"
            day_str = f"{year}_{month:02d}_{day:02d}"
            
            func = partial(process_hour, year=year, month=month, day=day, download_dir=download_dir, repo_ids=repo_ids)
            with Pool(processes=min(cpu_count(), 24)) as pool:
                for result in tqdm(pool.imap_unordered(func, range(24)), total=24, desc="Processing hours"):
                    if result:
                        contributors = result
                        for repo_id, contributors_set in contributors.items():
                            all_contributors[repo_id].update(contributors_set)
            break
        except Exception as e:
            print(f"Error processing day {day_str}: {e}")
            raise

    # Save comment_count with repo_id and html_url
    rows = []
    for repo_id, contributors_set in all_contributors.items():
        rows.append({
            "repo_id": repo_id,
            "contributors": list(contributors_set)
        })
    # save to json
    with open(f"contributors_files/contributors_{year}_{month:02d}_all_days.json", "w") as f:
        json.dump(rows, f, indent=4)