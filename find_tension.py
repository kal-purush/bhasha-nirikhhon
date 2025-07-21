import argparse
import os
import gzip
import json
from collections import defaultdict
import time
import pandas as pd
import regex as re
from multiprocessing import Pool, cpu_count
from functools import partial
from tqdm import tqdm

parser = argparse.ArgumentParser(description="Process GH Archive event counts and language stats.")
parser.add_argument("--year", type=int, required=True)
parser.add_argument("--month", type=int, required=True)
args = parser.parse_args()

year = args.year
month = args.month

write_in_phrases = {
    "english": [
        r"write in english"
    ],
    "russian": [
        r"пишите по[- ]?русски",
        r"напишите на русском",
        r"write in russian"
    ],
    "chinese": [
        r"用中文写",
        r"写中文",
        r"write in chinese"
    ],
    "japanese": [
        r"日本語で書いて",
        r"write in japanese"
    ],
    "korean": [
        r"한국어로 작성",
        r"한국어로 써",
        r"write in korean"
    ]
}

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

def process_hour(hour, year, month, day, download_dir, ids):
    local_counts = defaultdict(int)
    matched_urls = []
    day_str = f"{year}-{month:02d}-{day:02d}"
    filename = f"{day_str}-{hour}.json.gz"
    filepath = os.path.join(download_dir, filename)
    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    event = json.loads(line)
                    event_type = event.get("type")
                    if event_type in {"IssuesEvent", "IssueCommentEvent"}:
                        if 'bot' not in event.get("actor", {}).get("login", ""):
                            payload = event.get("payload", {})
                            title, body, html_url = "", "", ""
                            if event_type == "IssuesEvent":
                                issue = payload.get("issue", {})
                                title = issue.get("title", "")
                                body = issue.get("body", "")
                                html_url = issue.get("html_url", "")
                            elif event_type == "IssueCommentEvent":
                                issue = payload.get("comment", {})
                                title = issue.get("title", "")
                                body = issue.get("body", "")
                                html_url = issue.get("html_url", "")
                            message = f"{title}\n{body}".strip()
                            for lang, patterns in write_in_phrases.items():
                                for pattern in patterns:
                                    if re.search(pattern, message, re.IGNORECASE):
                                        local_counts[f"write_in_{pattern}"] += 1
                                        # print(message, html_url)
                                        matched_urls.append((pattern, html_url))
                except Exception:
                    continue
    except Exception:
        return None
    return local_counts, matched_urls

if __name__ == "__main__":
    for day in range(1, 32):
        try:
            local_counts = defaultdict(int)
            local_urls = []
            start_time = time.time()
            download_dir = f"data/gharchive_{year}_{month:02d}"
            day_str = f"{year}_{month:02d}_{day:02d}"
            df = pd.read_csv(f"id_files/combined_non_english_ids_{day_str}.csv")
            ids = df['id'].tolist()
            eng_df = pd.read_csv(f"id_files/combined_english_ids_{day_str}.csv")
            ids.extend(eng_df['id'].tolist())
            print(f"Processing day {day_str} with {len(ids)} IDs...")
            func = partial(process_hour, year=year, month=month, day=day, download_dir=download_dir, ids=ids)
            with Pool(processes=min(cpu_count(), 24)) as pool:
                for result in tqdm(pool.imap_unordered(func, range(24)), total=24, desc="Processing hours"):
                    if result:
                        counts, urls = result
                        for k, v in counts.items():
                            local_counts[k] += v
                        if urls:
                            local_urls.extend(urls)
            print(f"Day {day_str} processed in {time.time() - start_time:.2f} seconds. Counts: {dict(local_counts)}")
            df_counts = pd.DataFrame.from_dict(local_counts, orient='index', columns=['count']).reset_index()
            df_counts.columns = ['phrase', 'count']
            os.makedirs("phrase_stats", exist_ok=True)
            # print(local_urls)
            df_counts.to_csv(f"phrase_stats/write_phrase_count_{year}_{month:02d}_{day:02d}.csv", index=False)
            pd.DataFrame(local_urls, columns=["phrase", "url"]).to_csv(f"phrase_stats/write_phrase_urls_{year}_{month:02d}_{day:02d}.csv", index=False)
        except Exception as e:
            print(f"Error processing day {day_str}: {e}")
            continue


# import argparse
# import os
# import gzip
# import json
# from collections import defaultdict
# import time
# import pandas as pd
# import regex as re
# from multiprocessing import Pool, cpu_count
# from functools import partial
# from tqdm import tqdm

# parser = argparse.ArgumentParser(description="Process GH Archive for language insecurity phrases.")
# parser.add_argument("--year", type=int, required=True)
# parser.add_argument("--month", type=int, required=True)
# args = parser.parse_args()

# year = args.year
# month = args.month

# language_skill_phrases = {
#     "language_insecurity": [
#         r"my english is( not)?( very)? (good|perfect|great)",
#         r"sorry for my (bad|poor) (english|language)",
#         r"i'?m not good at (english|language)",
#         r"my (english|language) sucks",
#         r"excuse my (english|language)",
#         r"forgive my (english|language)"
#     ]
# }

# def clean_body_text(text):
#     if not text:
#         return ""
#     lines = text.splitlines()
#     cleaned_lines = []
#     for line in lines:
#         if re.search(r'https?://\S+|www\.\S+', line):
#             continue
#         if re.search(r'!\[.*?\]\(.*?\)', line):
#             continue
#         line = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', line)
#         line = re.sub(r'`{1,3}[^`]+`{1,3}', '', line)
#         line = re.sub(r'```[\s\S]+?```', '', line)
#         line = re.sub(r'[^\p{L}\p{N}\s.,!?\'\"-]', '', line)
#         cleaned_lines.append(line.strip())
#     return ' '.join(cleaned_lines).strip()

# def process_hour(hour, year, month, day, download_dir, ids):
#     local_counts = defaultdict(int)
#     matched_urls = []
#     day_str = f"{year}-{month:02d}-{day:02d}"
#     filename = f"{day_str}-{hour}.json.gz"
#     filepath = os.path.join(download_dir, filename)
#     try:
#         with gzip.open(filepath, 'rt', encoding='utf-8') as f:
#             for line in f:
#                 try:
#                     event = json.loads(line)
#                     event_type = event.get("type")
#                     if event_type in {"IssuesEvent", "IssueCommentEvent"}:
#                         if 'bot' not in event.get("actor", {}).get("login", ""):
#                             payload = event.get("payload", {})
#                             title, body, html_url = "", "", ""
#                             if event_type == "IssuesEvent":
#                                 issue = payload.get("issue", {})
#                                 title = issue.get("title", "")
#                                 body = issue.get("body", "")
#                                 html_url = issue.get("html_url", "")
#                             elif event_type == "IssueCommentEvent":
#                                 issue = payload.get("comment", {})
#                                 title = issue.get("title", "")
#                                 body = issue.get("body", "")
#                                 html_url = issue.get("html_url", "")
#                             message = f"{title}\n{body}".strip()
#                             message = clean_body_text(message)
#                             for tag, patterns in language_skill_phrases.items():
#                                 for pattern in patterns:
#                                     if re.search(pattern, message, re.IGNORECASE):
#                                         local_counts[f"{tag}::{pattern}"] += 1
#                                         matched_urls.append((pattern, html_url))
#                 except Exception:
#                     continue
#     except Exception:
#         return None
#     return local_counts, matched_urls

# if __name__ == "__main__":
#     for day in range(1, 32):
#         try:
#             local_counts = defaultdict(int)
#             local_urls = []
#             start_time = time.time()
#             download_dir = f"data/gharchive_{year}_{month:02d}"
#             day_str = f"{year}_{month:02d}_{day:02d}"
#             df = pd.read_csv(f"id_files/combined_non_english_ids_{day_str}.csv")
#             ids = df['id'].tolist()
#             eng_df = pd.read_csv(f"id_files/combined_english_ids_{day_str}.csv")
#             ids.extend(eng_df['id'].tolist())
#             print(f"Processing day {day_str} with {len(ids)} IDs...")
#             func = partial(process_hour, year=year, month=month, day=day, download_dir=download_dir, ids=ids)
#             with Pool(processes=min(cpu_count(), 24)) as pool:
#                 for result in tqdm(pool.imap_unordered(func, range(24)), total=24, desc="Processing hours"):
#                     if result:
#                         counts, urls = result
#                         for k, v in counts.items():
#                             local_counts[k] += v
#                         if urls:
#                             local_urls.extend(urls)
#             print(f"Day {day_str} processed in {time.time() - start_time:.2f} seconds. Counts: {dict(local_counts)}")
#             df_counts = pd.DataFrame.from_dict(local_counts, orient='index', columns=['count']).reset_index()
#             df_counts.columns = ['phrase', 'count']
#             os.makedirs("phrase_stats", exist_ok=True)
#             df_counts.to_csv(f"phrase_stats/lang_insecurity_count_{year}_{month:02d}_{day:02d}.csv", index=False)
#             pd.DataFrame(local_urls, columns=["phrase", "url"]).to_csv(f"phrase_stats/lang_insecurity_urls_{year}_{month:02d}_{day:02d}.csv", index=False)
#         except Exception as e:
#             print(f"Error processing day {day_str}: {e}")
#             continue
