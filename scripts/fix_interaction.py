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

def clean_body_text(text):
    if not text:
        return ""

    lines = text.splitlines()
    cleaned_lines = []

    for line in lines:
        if re.search(r'https?://\S+|www\.\S+', line):
            continue
        if re.search(r'!\[.*?\]\(.*?\)', line):
            continue

        line = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', line)
        line = re.sub(r'`{1,3}[^`]+`{1,3}', '', line)
        line = re.sub(r'```[\s\S]+?```', '', line)
        line = re.sub(r'[^\p{L}\p{N}\s.,!?\'\"-]', '', line)

        cleaned_lines.append(line.strip())

    return ' '.join(cleaned_lines).strip()

def recursive_defaultdict_to_dict(d):
    if isinstance(d, defaultdict):
        d = {k: recursive_defaultdict_to_dict(v) for k, v in d.items()}
    elif isinstance(d, list):
        d = [recursive_defaultdict_to_dict(i) for i in d]
    return d

def process_hour(hour, year, month, day, download_dir, df, message_dict):
    day_str = f"{year}-{month:02d}-{day:02d}"
    filename = f"{day_str}-{hour}.json.gz"
    filepath = os.path.join(download_dir, filename)

    # Store repo_id and html_url with each metric
    comment_count = defaultdict(lambda: defaultdict(lambda: {"repo_id": None, "html_url": None, "comment_count": 0}))
    # comment_count = defaultdict(int)
    tcomment_count = 0
    comment_ids =[]
    push_count = defaultdict(int)
    issue_close_durations = defaultdict(list)  # language -> list of dicts with repo_id, html_url, duration
    pr_merge_durations = defaultdict(list)     # language -> list of dicts with repo_id, html_url, duration
    time_to_first_interactions = defaultdict(list)  # language -> list of dicts with repo_id, html_url, tfi
    not_merged_prs = defaultdict(int)

    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    event = json.loads(line)
                    event_type = event.get("type")
                    if event_type in {"IssueCommentEvent","PushEvent"}:
                        if 'bot' not in event.get("actor", {}).get("login", ""):
                            payload = event.get("payload", {})
                            repo_id = event.get("repo", {}).get("id", None)
                            if event_type == "IssueCommentEvent":
                                comment = payload.get("comment", {})
                                title = comment.get("title", "")
                                body = comment.get("body", "")
                                item_id = comment.get("id", None)
                                html_url = comment.get("html_url", "")
                                if 'bot' in comment.get("user", {}).get("login", ""):
                                    continue
                                issue = payload.get("issue", {})
                                number_of_comments = issue.get("comments", -1)
                                issue_id = issue.get("id", None)
                                cleaned_body = clean_body_text(body)
                                message = f"{title}\n{cleaned_body}".strip()
                                language = message_dict.get(message, None)
                                # language_row = df[df['message'] == message]
                                # Increment comment count and store repo_id and html_url
                                # comment_count[language][issue_id]["comment_count"] += 1
                                # comment_count[language][issue_id]["repo_id"] = repo_id
                                # comment_count[language][issue_id]["html_url"] = html_url
                                # comment_count[repo_id]+= 1
                                if language:
                                    for lang in language:
                                        comment_count[lang][issue_id]["comment_count"] += 1
                                        comment_count[lang][issue_id]["repo_id"] = repo_id
                                        comment_count[lang][issue_id]["html_url"] = html_url
                                        tcomment_count += 1
                                        
                            elif event_type == "PushEvent":
                                push_count[repo_id] += 1
                                
                            
                except Exception:
                    pass
    except Exception:
        return None
    return (
        recursive_defaultdict_to_dict(comment_count), 
        dict(push_count)
    )



if __name__ == "__main__":
    ## create interactions_files directory if it doesn't exist
    if not os.path.exists("interactions_files_new"):
        os.makedirs("interactions_files_new")
    all_comment_count = defaultdict(lambda: defaultdict(lambda: {"repo_id": None, "html_url": None, "comment_count": 0}))
    # all_comment_count = defaultdict(int)
    # all_comment_counts = []
    all_push_count = defaultdict(int)
    all_issue_close_durations = defaultdict(list)
    all_pr_merge_durations = defaultdict(list)
    all_time_to_first_interactions = defaultdict(list)
    all_not_merged_prs = defaultdict(int)
    for day in range(1, 32):  # Change to range(1, 33) for all days in month
        try:
            df = pd.read_csv(f"resul_files/messages_with_languages_{year}_{month:02d}_{day:02d}.csv")
            # print(f"Processing {year}-{month:02d}-{day:02d} with {len(df)} messages.")
            df = df.drop_duplicates(keep='first').reset_index(drop=True)
            # print(f"Processing {year}-{month:02d}-{day:02d} with {len(df)} messages.")
            # df = df[df['language'] != 'english'].reset_index(drop=True)
            message_dict = defaultdict(set)
            for message, language in zip(df['message'], df['language']):
                message_dict[message].add(language)
            print(f"Loaded {len(message_dict)} messages for {year}-{month:02d}-{day:02d}")
            download_dir = f"data/gharchive_{year}_{month:02d}"
            day_str = f"{year}_{month:02d}_{day:02d}"
            
            func = partial(process_hour, year=year, month=month, day=day, download_dir=download_dir, df=df, message_dict=message_dict)
            with Pool(processes=min(cpu_count(), 24)) as pool:
                for result in tqdm(pool.imap_unordered(func, range(24)), total=24, desc="Processing hours"):
                    if result:
                        comment_count, push_count = result
                        for lang, issues in comment_count.items():
                            for issue_id, data in issues.items():
                                all_comment_count[lang][issue_id]["comment_count"] += data["comment_count"]
                                all_comment_count[lang][issue_id]["repo_id"] = data["repo_id"]
                                all_comment_count[lang][issue_id]["html_url"] = data.get("html_url", None)
                        
                        for repo_id, count in push_count.items():
                            all_push_count[repo_id] += count
            # break
        except Exception as e:
            print(f"Error processing day {day_str}: {e}")
            # raise


    rows = []
    for lang, issues in all_comment_count.items():
        for issue_id, data in issues.items():
            rows.append({
                "language": lang,
                "issue_id": issue_id,
                "repo_id": data["repo_id"],
                "html_url": data.get("html_url", None),
                "comment_count": data["comment_count"]
            })
    pd.DataFrame(rows).to_csv(f"interactions_files_new/comment_count_{year}_{month:02d}_all_days.csv", index=False)

    # Save push_count
    rows = []
    for repo_id, count in all_push_count.items():
        rows.append({"repo_id": repo_id, "push_count": count})
    pd.DataFrame(rows).to_csv(f"interactions_files_new/push_count_{year}_{month:02d}_all_days.csv", index=False)
