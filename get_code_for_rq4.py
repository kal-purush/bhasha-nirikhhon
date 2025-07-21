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
parser.add_argument("--target", type=int, required=True)
# parser.add_argument("--day", type=int, required=True)
args = parser.parse_args()

year = args.year
month = args.month
target = args.target

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}

# def run_cmd(cmd, cwd=None):
#     try:
#         return subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True, cwd=cwd)
#     except subprocess.CalledProcessError:
#         return ""

valid_exts = (".py", ".ts", ".js", ".java", ".cs")
def is_new_file(lines):
    for line in lines:
        if line.startswith("new file mode"):
            return True
    return False

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
        if "node_modules" in fpath:
            continue
        ext = Path(fpath).suffix.lower()
        if ext not in valid_exts:
            continue
        is_new = is_new_file(lines)
        if not is_new:
            continue
        # print("DIFF ==> ", diff)
        patch_lines = [line[1:] for line in lines[1:] if line.startswith("+") and not line.startswith("+++")]
        patch_code = "\n".join(patch_lines)
        if not patch_lines:
            continue
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
        print("Command issue")
        return -1

    if "Username for" in result.stderr or "Authentication failed" in result.stderr:
        print("USERNAME")
        return -1

    if not os.path.isdir(folder):
        print("Not directory issue")
        return -1

    # run_cmd(["git", "clone", "--filter=blob:none", "--no-checkout", repo_url, folder])


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
    ] + ["--"] + [f"*.{ext[1:]}" for ext in valid_exts]
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
        print("git log Command issue")
        return -1

    if len(patch_output) == 0:
        # print(f"No matching patches in {full_name}")
        try:
            shutil.rmtree(folder)
            print(f"Deleted {folder}\n")
        except:
            pass
        print("No patch found issue")
        return -1

    inputs = [commit for commit in read_commits_stream(patch_output)]
    for commit in inputs:
        if "new file mode" in commit:
            try:
                code_obj = process_commit_data(commit)
                if code_obj:
                    # Save code to file with extension in log_file_path
                    # out_path = f"{os.path.splitext(log_file_path)[0]}_{code_obj['commit']}{code_obj['ext']}"
                    # print(out_path)
                    # with open(out_path, "w", encoding="utf-8") as f:
                    #     f.write(code_obj['code'])
                    shutil.rmtree(folder)
                    return code_obj
            except:
                pass
    try:
        shutil.rmtree(folder)
        print(f"Deleted {folder}\n")
    except:
        pass
    return -1

# if __name__ == "__main__":
#     log_output_dir = os.path.join("rq4_logs", f"{year}-{month:02d}")
#     os.makedirs(log_output_dir, exist_ok=True)

#     df = pd.read_csv(f"sampled_repos_{year}_{month:02d}.csv")
#     if 'index' in df.columns:
#         df = df.drop(columns=['index'])

#     # df_sample = df.sample(n=20000, random_state=42).reset_index(drop=True)  # sample enough to reach all targets
#     df_sample = df
#     task_args = [(row['id'], row['name'], log_output_dir) for _, row in df_sample.iterrows()]

#     success_count = defaultdict(int)
#     target_count = 100
#     extensions_needed = {".py", ".ts", ".js", ".java", ".cs"}
#     results = []

#     def process_wrapper(args):
#         repo_id, repo_name, log_dir = args
#         return process_repo(repo_id, repo_name, log_dir)


#     with Pool(processes=min(cpu_count(), 32)) as pool:
#         for result in tqdm(pool.imap_unordered(process_wrapper, task_args), total=len(task_args), desc="Processing Repos"):
#             if isinstance(result, dict):
#                 ext = result.get("ext")
#                 if ext in extensions_needed:
#                     if success_count[ext] < target_count:
#                         out_path = f"{os.path.splitext(os.path.join(log_output_dir, f'{result['commit']}'))[0]}{ext}"
#                         with open(out_path, "w", encoding="utf-8") as f:
#                             f.write(result["code"])
#                         success_count[ext] += 1

#             # stop when all ext targets are reached
#             if all(success_count[ext] >= target_count for ext in extensions_needed):
#                 print("Reached 100 for each extension.")
#                 break


import os
import pandas as pd
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# maps language names to file extensions
language_to_ext = {
    "Python": ".py",
    "TypeScript": ".ts",
    "JavaScript": ".js",
    "Java": ".java",
    "C#": ".cs"
}

# targets per extension
# targets = {
#     ".py": 22,
#     ".java": 52,
#     ".ts": 85,
#     ".cs": 85
# }

targets = {
    ".ts": 22,
}

if __name__ == "__main__":
    log_output_dir = os.path.join("rq4_logs_new", f"{year}-{month:02d}")
    os.makedirs(log_output_dir, exist_ok=True)

    df = pd.read_csv(f"sampled_repos_{year}_{month:02d}.csv")
    if 'index' in df.columns:
        df = df.drop(columns=['index'])

    def process_wrapper(args):
        repo_id, repo_name, log_dir = args
        return process_repo(repo_id, repo_name, log_dir)

    for lang, ext in language_to_ext.items():
        if ext not in targets:
            continue

        # target_count = targets[ext]
        target_count = target+10
        print(f"Processing {lang} files ({ext}), target: {target_count}")

        df_lang = df[df['language'] == lang].sample(frac=1).reset_index(drop=True)
        task_args = [(row['id'], row['name'], log_output_dir) for _, row in df_lang.iterrows()]

        count = 0
        with Pool(processes=min(cpu_count(), 32)) as pool:
            for result in tqdm(pool.imap_unordered(process_wrapper, task_args), total=len(task_args), desc=f"{ext}"):
                if isinstance(result, dict) and result.get("ext") == ext:
                    out_path = os.path.splitext(os.path.join(log_output_dir, result['commit']))[0] + ext
                    with open(out_path, "w", encoding="utf-8") as f:
                        f.write(result["code"])
                    count += 1

                if count >= target_count:
                    print(f"Reached target for {ext}")
                    break


# if __name__ == "__main__":
#     log_output_dir = os.path.join("rq4_logs_new", f"{year}-{month:02d}")
#     os.makedirs(log_output_dir, exist_ok=True)

#     df = pd.read_csv(f"sampled_repos_{year}_{month:02d}.csv")
#     if 'index' in df.columns:
#         df = df.drop(columns=['index'])

#     ext = ".ts"
#     lang = "TypeScript"
#     target_count = target + 10
#     print(f"Processing {lang} files ({ext}), target: {target_count}")
#     df_lang = df[df['language'] == lang].reset_index(drop=True)
#     # print(len(df_lang))
#     count = 0
#     for i in range(len(df_lang)):
#         print(f"processing ......{i}")
#         id = df.iloc[i]['id']
#         name = df.iloc[i]['name']
#         result = process_repo(id, name, log_output_dir) 
#         if isinstance(result, dict) and result.get("ext") == ext:
#             out_path = os.path.splitext(os.path.join(log_output_dir, result['commit']))[0] + ext
#             with open(out_path, "w", encoding="utf-8") as f:
#                 f.write(result["code"])
#                 f.flush()
#             count += 1

#         if count >= target_count:
#             print(f"Reached target for {ext}")
#             break



