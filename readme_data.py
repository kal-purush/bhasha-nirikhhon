import argparse
from collections import defaultdict
from multiprocessing import Pool, cpu_count
import os
from pathlib import Path
import re
import subprocess
import shutil
import pandas as pd
from datetime import datetime, timedelta
import calendar
from itertools import islice

from multiprocessing import Pool, cpu_count
from tqdm import tqdm

def process_wrapper(args):
    repo_id, repo_name, log_dir = args
    return process_repo(repo_id, repo_name, log_dir)
# # CLI arguments
parser = argparse.ArgumentParser(description="Process GH Archive event counts and language stats.")
parser.add_argument("--year", type=int, required=True)
parser.add_argument("--month", type=int, required=True)
# parser.add_argument("--day", type=int, required=True)
args = parser.parse_args()

year = args.year
month = args.month

MAX_SIZE_BYTES = 5 * 1024 * 1024 * 1024  # 5GB
REPO_SIZE_LIMIT_BYTES = 1 * 1024 * 1024 * 1024  # 1GB
ALLOWED_LANGUAGES = {"Python", "JavaScript", "TypeScript"}
MAX_REQUESTS_PER_HOUR = 4095
MIN_SECONDS_BETWEEN_REQUESTS = 3600 / MAX_REQUESTS_PER_HOUR  # ~0.88s

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}

# def run_cmd(cmd, cwd=None):
#     try:
#         return subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True, cwd=cwd)
#     except subprocess.CalledProcessError:
#         return ""

def is_new_file(lines):
    for line in lines:
        if line.startswith("new file mode"):
            return True
    return False

# def parse_patch_by_line(patch, ext):
#     entities = defaultdict(list)
#     for line in patch.splitlines():
#         try:
#             if not line.strip():
#                 continue
#             ents = extract_entities(line + "\n", ext)
#             for key in ents:
#                 entities[key].extend(ents[key])
#         except:
#             continue
#     return entities

def process_commit_data(args):
    commit = args
    lines = commit.splitlines()
    if not lines or not lines[0].startswith("commit") or "[bot]" in lines[1]:
        return None
    entries = []
    commit_hash = lines[0].split()[1]
    diffs = re.split(r'diff --git a/', commit)
    date = lines[2] if len(lines) > 2 else ""

    for diff in diffs[1:]:
        lines = diff.splitlines()
        if not lines:
            continue
        fpath = lines[0].split()[0]
        ext = Path(fpath).suffix.lower()
        patch_lines = [line[1:] for line in lines[1:] if line.startswith("+") and not line.startswith("+++")]
        patch_code = "\n".join(patch_lines)
        if not patch_lines:
            continue
        char_count_no_whitespace = len(re.sub(r"\s", "", patch_code))
        if char_count_no_whitespace>500:
            return{
                "date": date,
                "commit": commit_hash,
                "file": fpath,
                "ext": ext,
                "code":patch_code
            }
        
    return None

def read_commits_stream(data):
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="replace")

    commit = []
    for line in data.splitlines(keepends=True):
        if line.startswith("commit ") and commit:
            yield ''.join(commit)
            commit = [line]
        else:
            commit.append(line)
    if commit:
        yield ''.join(commit)

def run_cmd(cmd):
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            input='',
            timeout=30
        )
        return result
    except subprocess.TimeoutExpired:
        return None

def process_repo(repo_id, full_name, log_dir):
    log_file_path = os.path.join(log_dir, f"{repo_id}.txt")

    # if os.path.exists(log_file_path):
    #     print(f"Skipping {full_name} (already done)")
    #     return 0

    repo_url = f"https://github.com/{full_name}.git"
    folder = f"repo_{repo_id}"

    # print(f"Cloning {full_name}...")
    result = run_cmd(["git", "clone", "--filter=blob:none", "--no-checkout", repo_url, folder])

    if result is None:
        return -1

    if "Username for" in result.stderr or "Authentication failed" in result.stderr:
        return -1

    if not os.path.isdir(folder):
        return -1

    # run_cmd(["git", "clone", "--filter=blob:none", "--no-checkout", repo_url, folder])
    if not os.path.isdir(folder):
        # print(f"Failed to clone {full_name}")
        return -1

    # print(f"Checking commits for {full_name} in {year}-{month:02d}...")

    # build date range
    start_date = datetime(year, month, 1)
    after_date = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")
    last_day = calendar.monthrange(year, month)[1]
    before_date = datetime(year, month, last_day).strftime("%Y-%m-%d")

    # build git log command
    cmd = [
        "git", "log",
        f"--after={after_date}",
        f"--before={before_date}",
        "--patch",
        "--pretty"
    ] + ["--", "README*"]
    # print(cmd)
    try:
        patch_output = subprocess.check_output(
            cmd,
            cwd=folder,
            stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError:
        # print(f"Failed to run git log (patch) for {full_name}")
        # shutil.rmtree(folder)
        return -1

    if len(patch_output) == 0:
        # print(f"No matching patches in {full_name}")
        try:
            shutil.rmtree(folder)
            print(f"Deleted {folder}\n")
        except:
            pass
        return -1

    inputs = [commit for commit in read_commits_stream(patch_output)]
    for commit in inputs:
        try:
            code_obj = process_commit_data(commit)
            if code_obj:
                # Save code to file with extension in log_file_path
                out_path = f"{os.path.splitext(log_file_path)[0]}_{code_obj['commit']}{code_obj['ext']}"
                # print(out_path)
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(code_obj['code'])
                shutil.rmtree(folder)
                return 1
        except:
            pass
    try:
        shutil.rmtree(folder)
        print(f"Deleted {folder}\n")
    except:
        pass
    return -1

if __name__ == "__main__":
    log_output_dir = os.path.join("readme_logs", f"{year}-{month:02d}")
    os.makedirs(log_output_dir, exist_ok=True)
    df = pd.read_csv(f"repos_{year}_{month:02d}.csv")
    if 'index' in df.columns:
        df = df.drop(columns=['index'])
    
    df_sample = df.sample(n=10000, random_state=42).reset_index(drop=True)  # sample more than 385
    task_args = [(row['id'], row['name'], log_output_dir) for _, row in df_sample.iterrows()]

    successful = 0
    results = []

    with Pool(processes=min(cpu_count(), 32)) as pool:
        for result in tqdm(pool.imap_unordered(process_wrapper, task_args), total=len(task_args), desc="Processing Repos"):
            try:
                if result == 1:
                    successful += 1
                    if successful >= 385:
                        print("Reached 385 successful clones.")
                        break
            except Exception as e:
                print(f"Error processing a repo: {e}")
                continue

    # count=0
    # successful = 0
    # for i in range(len(df_sample)):
    #     # print(df_sample.iloc[i])
    #     repo_name = df_sample.iloc[i]['name']
    #     id = df_sample.iloc[i]['id']
    #     print(repo_name, id)
    #     return_val = process_repo(id, repo_name, log_output_dir)
    #     if return_val==1:
    #         successful+=1
    #     count+=1
    #     if count>10:
    #         print("Total Successful ===> ", successful)
    #         break