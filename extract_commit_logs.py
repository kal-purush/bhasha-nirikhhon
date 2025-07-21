import csv
import os
import subprocess
import shutil
import time
import requests

MAX_SIZE_BYTES = 5 * 1024 * 1024 * 1024  # 5GB
REPO_SIZE_LIMIT_BYTES = 1 * 1024 * 1024 * 1024  # 1GB
ALLOWED_LANGUAGES = {"Python", "JavaScript", "TypeScript"}
MAX_REQUESTS_PER_HOUR = 4095
MIN_SECONDS_BETWEEN_REQUESTS = 3600 / MAX_REQUESTS_PER_HOUR  # ~0.88s

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}

def run_cmd(cmd, cwd=None):
    try:
        return subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True, cwd=cwd)
    except subprocess.CalledProcessError:
        return ""

def get_repo_size_bytes(full_name):
    url = f"https://api.github.com/repos/{full_name}"
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 403:
            print("Rate limit hit. Sleeping for 60 seconds...", full_name, response.headers)
            if response.headers['X-RateLimit-Remaining']==0:
                reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                sleep_seconds = max(0, reset_time - int(time.time()) + 1)
                print(f"Sleeping for {sleep_seconds} seconds until rate limit resets...")
                time.sleep(sleep_seconds)
                return get_repo_size_bytes(full_name)
            else:
                return None
            # return get_repo_size_bytes(full_name)
        elif response.status_code != 200:
            print(f"Failed to get size for {full_name} (status: {response.status_code})")
            return None
        data = response.json()
        return data.get("size", 0) * 1024  # GitHub reports size in KB
    except Exception as e:
        print(f"Error getting repo size for {full_name}: {e}")
        return None

def process_repo(repo_id, full_name, log_dir):
    log_file_path = os.path.join(log_dir, f"{repo_id}.txt")

    if os.path.exists(log_file_path):
        print(f"Skipping {full_name} (already done)")
        return

    size_bytes = get_repo_size_bytes(full_name)
    time.sleep(MIN_SECONDS_BETWEEN_REQUESTS)

    if size_bytes is None:
        print(f"Skipping {full_name} (API error)")
        return
    if size_bytes > REPO_SIZE_LIMIT_BYTES:
        print(f"Skipping {full_name} (size > 1GB)")
        return

    repo_url = f"https://github.com/{full_name}.git"
    folder = f"repo_{repo_id}"

    print(f"Cloning {full_name}...")
    run_cmd(["git", "clone", repo_url, folder])
    if not os.path.isdir(folder):
        print(f"Failed to clone {full_name}")
        return

    print(f"Checking log size for {full_name}...")
    try:
        log_output = subprocess.check_output(
            ["git", "log", "--patch", "--pretty"],
            cwd=folder,
            stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError:
        print(f"Failed to run git log for {full_name}")
        shutil.rmtree(folder)
        return

    if len(log_output) > MAX_SIZE_BYTES:
        print(f"Skipping {full_name} (log size > 5GB)")
        shutil.rmtree(folder)
        return

    print(f"Saving log to {log_file_path}...")
    with open(log_file_path, "wb") as f:
        f.write(log_output)

    shutil.rmtree(folder)
    print(f"Deleted {folder}\n")

# CSV input
csv_file = "sampled_30k_repos.csv"
log_output_dir = "logs"
os.makedirs(log_output_dir, exist_ok=True)

with open(csv_file, newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            language = row["language"]
            if language not in ALLOWED_LANGUAGES:
                continue

            repo_id = row["id"]
            full_name = row["name"]
            process_repo(repo_id, full_name, log_output_dir)
        except:
            pass