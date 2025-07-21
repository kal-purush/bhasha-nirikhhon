import calendar
import csv
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
import os
import subprocess
import shutil
import time
import pandas as pd
import requests
from tqdm import tqdm
import argparse

MAX_SIZE_BYTES = 5 * 1024 * 1024 * 1024  # 5GB
REPO_SIZE_LIMIT_BYTES = 1 * 1024 * 1024 * 1024  # 1GB
ALLOWED_LANGUAGES = {"Python", "JavaScript", "TypeScript"}
MAX_REQUESTS_PER_HOUR = 4095
MIN_SECONDS_BETWEEN_REQUESTS = 3600 / MAX_REQUESTS_PER_HOUR  # ~0.88s

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}

parser = argparse.ArgumentParser(description="Process GH Archive event counts and language stats.")
parser.add_argument("--year", type=int, required=True)
parser.add_argument("--month", type=int, required=True)
# parser.add_argument("--target", type=int, required=True)
# parser.add_argument("--day", type=int, required=True)
args = parser.parse_args()

year = args.year
month = args.month
target = 17

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


# def process_repo(repo_id, full_name, log_dir):
#     log_file_path = os.path.join(log_dir, f"{repo_id}.txt")

#     if os.path.exists(log_file_path):
#         print(f"Skipping {full_name} (already done)")
#         return

#     size_bytes = get_repo_size_bytes(full_name)
#     time.sleep(MIN_SECONDS_BETWEEN_REQUESTS)

#     if size_bytes is None:
#         print(f"Skipping {full_name} (API error)")
#         return
#     if size_bytes > REPO_SIZE_LIMIT_BYTES:
#         print(f"Skipping {full_name} (size > 1GB)")
#         return

#     repo_url = f"https://github.com/{full_name}.git"
#     folder = f"repo_{repo_id}"

#     print(f"Cloning {full_name}...")
#     run_cmd(["git", "clone", repo_url, folder])
#     if not os.path.isdir(folder):
#         print(f"Failed to clone {full_name}")
#         return

#     print(f"Checking log size for {full_name}...")
#     try:
#         log_output = subprocess.check_output(
#             ["git", "log", "--patch", "--pretty"],
#             cwd=folder,
#             stderr=subprocess.DEVNULL
#         )
#     except subprocess.CalledProcessError:
#         print(f"Failed to run git log for {full_name}")
#         shutil.rmtree(folder, ignore_errors=True)
#         return

#     if len(log_output) > MAX_SIZE_BYTES:
#         print(f"Skipping {full_name} (log size > 5GB)")
#         shutil.rmtree(folder, ignore_errors=True)
#         return

#     print(f"Saving log to {log_file_path}...")
#     with open(log_file_path, "wb") as f:
#         f.write(log_output)

#     shutil.rmtree(folder, ignore_errors=True)
#     print(f"Deleted {folder}\n")


def process_repo(repo_id, full_name, log_dir):
    log_file_path = os.path.join(log_dir, f"{repo_id}.txt")

    if os.path.exists(log_file_path):
        print(f"Skipping {full_name} (already done)")
        return 0

    repo_url = f"https://github.com/{full_name}.git"
    folder = f"repo_{repo_id}"

    print(f"Cloning {repo_url}, {repo_id}")
    result = run_cmd(["git", "clone", "--filter=blob:none", "--no-checkout", repo_url, folder])

    if result is None:
        print("Command issue")
        return -1

    if "Username for" in result.stderr or "Authentication failed" in result.stderr:
        # print("USERNAME")
        shutil.rmtree(folder, ignore_errors=True)
        return -1

    if not os.path.isdir(folder):
        # print("Not directory issue")
        shutil.rmtree(folder, ignore_errors=True)
        return -1

    start_date = datetime(year, month, 1)
    after_date = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")
    last_day = calendar.monthrange(year, month)[1]
    before_date = datetime(year, month, last_day).strftime("%Y-%m-%d")

    # build git log command
    cmd = f'git log --after={after_date} --before={before_date} --patch --pretty -- *.ts'
    # print(cmd)
    try:
        # patch_output = subprocess.check_output(
        #     cmd,
        #     cwd=folder,
        #     stderr=subprocess.DEVNULL
        # )
        patch_output = subprocess.check_output(
            cmd,
            cwd=folder,
            stderr=subprocess.DEVNULL,
            shell=True,
            text=True
        )

    except Exception as e:
        shutil.rmtree(folder, ignore_errors=True)
        print(repo_id, full_name, e)
        return -1
    shutil.rmtree(folder, ignore_errors=True)
    return {"code": patch_output, "repo_id": repo_id}

language_to_ext = {
    "Python": ".py",
    "TypeScript": ".ts",
    "JavaScript": ".js",
    "Java": ".java",
    "C#": ".cs"
}

def process_wrapper(args):
    repo_id, repo_name, log_dir = args
    return process_repo(repo_id, repo_name, log_dir)

# if __name__ == "__main__":
#     # for month in [1,2,3,4,6,8]:
#     ext = ".ts"
#     lang = "TypeScript"
#     log_output_dir = os.path.join("rq4_git_logs_new", f"{year}-{month:02d}")
#     os.makedirs(log_output_dir, exist_ok=True)

#     df = pd.read_csv(f"sampled_repos_{year}_{month:02d}.csv")
#     if 'index' in df.columns:
#         df = df.drop(columns=['index'])

#     target_count = target+15
#     print(f"Processing {lang} files ({ext}), target: {target_count}")

#     # df_lang = df[df['language'] == lang].sample(n=200).reset_index(drop=True)
#     df_lang = df[df['language'] == lang]
#     df_lang = df_lang.sample(n=500).reset_index(drop=True)
#     task_args = [(row['id'], row['name'], log_output_dir) for _, row in df_lang.iterrows()]

#     count = 0
#     with Pool(processes=min(cpu_count(), 32)) as pool:
#         for result in tqdm(pool.imap_unordered(process_wrapper, task_args), total=len(task_args), desc=f"{ext}"):
#             if isinstance(result, dict):
#                 repo_id = result.get("repo_id")
#                 out_path = f"{log_output_dir}/{repo_id}.txt"
#                 with open(out_path, "w", encoding="utf-8") as f:
#                     f.write(result["code"])
#                 count += 1

#             if count >= target_count:
#                 print(f"Reached target for {ext}")
#                 break


if __name__ == "__main__":
    ext = ".py"
    lang = "Python"
    log_output_dir = os.path.join("rq4_git_logs_new", f"{year}-{month:02d}")
    os.makedirs(log_output_dir, exist_ok=True)

    df = pd.read_csv(f"sampled_repos_{year}_{month:02d}.csv")
    if 'index' in df.columns:
        df = df.drop(columns=['index'])

    target_count = target + 10
    print(f"Processing {lang} files ({ext}), target: {target_count}")
    df_lang = df[df['language'] == lang].reset_index(drop=True)

    task_args = [(row['id'], row['name'], log_output_dir) for _, row in df_lang.iterrows()]

    count = 0
    with Pool(processes=min(cpu_count(), 32)) as pool:
        for result in tqdm(pool.imap_unordered(process_wrapper, task_args), total=len(task_args), desc=f"{ext}"):
            try:
                if isinstance(result, dict):
                    repo_id = result.get("repo_id")
                    out_path = f"{log_output_dir}/{repo_id}.txt"
                    if len(result["code"]) > 0:
                        if "new file mode" in result["code"]:
                            with open(out_path, "w", encoding="utf-8") as f:
                                f.write(result["code"])
                            count += 1
                            print(f"Processed {count} files for {ext}")
                if count >= target_count:
                    print(f"Reached target for {ext}")
                    break
            except Exception as e:
                print(f"Error processing result: {e}")
                continue
