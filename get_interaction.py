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
                    if event_type in {"IssuesEvent", "IssueCommentEvent", "PullRequestEvent","PushEvent"}:
                        if 'bot' not in event.get("actor", {}).get("login", ""):
                            payload = event.get("payload", {})
                            repo_id = event.get("repo", {}).get("id", None)
                            if event_type == "IssuesEvent":
                                issue = payload.get("issue", {})
                                title = issue.get("title", "")
                                body = issue.get("body", "")
                                item_id = issue.get("id", None)
                                html_url = issue.get("html_url", "")
                                if 'bot' in issue.get("user", {}).get("login", ""):
                                    continue
                                action = payload.get("action", "")
                                if action == "closed":
                                    created_at = issue.get("created_at", "")
                                    closed_at = issue.get("closed_at", "")
                                    if created_at and closed_at:
                                        created_at = pd.to_datetime(created_at)
                                        closed_at = pd.to_datetime(closed_at)
                                        issue_close_duration = (closed_at - created_at).total_seconds()
                                        cleaned_body = clean_body_text(body)
                                        message = f"{title}\n{cleaned_body}".strip()
                                        language = message_dict.get(message, "english")
                                        issue_close_durations[language].append({
                                            "repo_id": repo_id,
                                            "issue_id": item_id,
                                            "html_url": html_url,
                                            "duration": issue_close_duration
                                        })
                            elif event_type == "IssueCommentEvent":
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
                                language = message_dict.get(message, "english")
                                # Increment comment count and store repo_id and html_url
                                comment_count[language][issue_id]["comment_count"] += 1
                                comment_count[language][issue_id]["repo_id"] = repo_id
                                comment_count[language][issue_id]["html_url"] = html_url
                                # Time to first interaction
                                if number_of_comments <= 1:
                                    # print(f"First interaction for issue {issue_id} in repo {repo_id} at {html_url}")
                                    created_at = issue.get("created_at", "")
                                    comment_created_at = comment.get("created_at", "")
                                    if created_at and comment_created_at:
                                        created_at = pd.to_datetime(created_at)
                                        comment_created_at = pd.to_datetime(comment_created_at)
                                        tfi = (comment_created_at - created_at).total_seconds()
                                        time_to_first_interactions[language].append({
                                            "repo_id": repo_id,
                                            "issue_id": issue_id,
                                            "html_url": html_url,
                                            "time_to_first_interaction": tfi
                                        })
                            elif event_type == "PushEvent":
                                push_count[repo_id] += 1
                            elif event_type == "PullRequestEvent":
                                pr = payload.get("pull_request", {})
                                title = pr.get("title", "")
                                body = pr.get("body", "")
                                item_id = pr.get("id", None)
                                html_url = pr.get("html_url", "")
                                if 'bot' in pr.get("user", {}).get("login", ""):
                                    continue
                                action = payload.get("action", "")
                                if action == "closed":
                                    created_at = pr.get("created_at", "")
                                    merged_at = pr.get("merged_at", "")
                                    cleaned_body = clean_body_text(body)
                                    message = f"{title}\n{cleaned_body}".strip()
                                    language = message_dict.get(message, "english")
                                    if merged_at:
                                        created_at = pd.to_datetime(created_at)
                                        merged_at = pd.to_datetime(merged_at)
                                        pr_merge_duration = (merged_at - created_at).total_seconds()
                                        pr_merge_durations[language].append({
                                            "repo_id": repo_id,
                                            "pr_id": item_id,
                                            "html_url": html_url,
                                            "duration": pr_merge_duration
                                        })
                                    else:
                                        not_merged_prs[language] += 1
                except Exception:
                    pass
    except Exception:
        return None
    return (
        recursive_defaultdict_to_dict(comment_count),
        dict(push_count),
        recursive_defaultdict_to_dict(issue_close_durations),
        recursive_defaultdict_to_dict(pr_merge_durations),
        recursive_defaultdict_to_dict(time_to_first_interactions),
        dict(not_merged_prs)
    )

if __name__ == "__main__":
    ## create interactions_files directory if it doesn't exist
    if not os.path.exists("interactions_files"):
        os.makedirs("interactions_files")
    all_comment_count = defaultdict(lambda: defaultdict(lambda: {"repo_id": None, "html_url": None, "comment_count": 0}))
    all_push_count = defaultdict(int)
    all_issue_close_durations = defaultdict(list)
    all_pr_merge_durations = defaultdict(list)
    all_time_to_first_interactions = defaultdict(list)
    all_not_merged_prs = defaultdict(int)
    for day in range(1, 32):  # Change to range(1, 33) for all days in month
        try:
            df = pd.read_csv(f"resul_files/messages_with_languages_{year}_{month:02d}_{day:02d}.csv")
            df = df.drop_duplicates(keep='first').reset_index(drop=True)
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
                        comment_count, push_count, issue_close_durations, pr_merge_durations, time_to_first_interactions, not_merged_prs = result
                        # Merge comment_count   
                        for lang, issues in comment_count.items():
                            for issue_id, data in issues.items():
                                all_comment_count[lang][issue_id]["comment_count"] += data["comment_count"]
                                all_comment_count[lang][issue_id]["repo_id"] = data["repo_id"]
                                all_comment_count[lang][issue_id]["html_url"] = data.get("html_url", None)

                        # Merge push_count
                        for repo_id, count in push_count.items():
                            all_push_count[repo_id] += count

                        # Merge issue_close_durations
                        for lang, durations in issue_close_durations.items():
                            all_issue_close_durations[lang].extend(durations)

                        # Merge pr_merge_durations
                        for lang, durations in pr_merge_durations.items():
                            all_pr_merge_durations[lang].extend(durations)

                        # Merge time_to_first_interactions
                        for lang, tfi_list in time_to_first_interactions.items():
                            all_time_to_first_interactions[lang].extend(tfi_list)

                        # Merge not_merged_prs
                        for lang, count in not_merged_prs.items():
                            all_not_merged_prs[lang] += count
            break
        except Exception as e:
            print(f"Error processing day {day_str}: {e}")
            raise

    # Save comment_count with repo_id and html_url
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
    pd.DataFrame(rows).to_csv(f"interactions_files/comment_count_{year}_{month:02d}_all_days.csv", index=False)

    # Save push_count
    rows = []
    for repo_id, count in all_push_count.items():
        rows.append({"repo_id": repo_id, "push_count": count})
    pd.DataFrame(rows).to_csv(f"interactions_files/push_count_{year}_{month:02d}_all_days.csv", index=False)

    # Save issue_close_durations with repo_id, issue_id, html_url
    rows = []
    for lang, durations in all_issue_close_durations.items():
        for d in durations:
            rows.append({
                "language": lang,
                "repo_id": d["repo_id"],
                "issue_id": d["issue_id"],
                "html_url": d.get("html_url", None),
                "issue_close_duration": d["duration"]
            })
    pd.DataFrame(rows).to_csv(f"interactions_files/issue_close_durations_{year}_{month:02d}_all_days.csv", index=False)

    # Save pr_merge_durations with repo_id, pr_id, html_url
    rows = []
    for lang, durations in all_pr_merge_durations.items():
        for d in durations:
            rows.append({
                "language": lang,
                "repo_id": d["repo_id"],
                "pr_id": d["pr_id"],
                "html_url": d.get("html_url", None),
                "pr_merge_duration": d["duration"]
            })
    pd.DataFrame(rows).to_csv(f"interactions_files/pr_merge_durations_{year}_{month:02d}_all_days.csv", index=False)

    # Save time_to_first_interactions with repo_id, issue_id, html_url
    rows = []
    for lang, tfi_list in all_time_to_first_interactions.items():
        for tfi in tfi_list:
            rows.append({
                "language": lang,
                "repo_id": tfi["repo_id"],
                "issue_id": tfi["issue_id"],
                "html_url": tfi.get("html_url", None),
                "time_to_first_interaction": tfi["time_to_first_interaction"]
            })
    pd.DataFrame(rows).to_csv(f"interactions_files/time_to_first_interactions_{year}_{month:02d}_all_days.csv", index=False)

    # Save not_merged_prs
    rows = []
    for lang, count in all_not_merged_prs.items():
        rows.append({"language": lang, "not_merged_prs": count})
    pd.DataFrame(rows).to_csv(f"interactions_files/not_merged_prs_{year}_{month:02d}_all_days.csv", index=False)