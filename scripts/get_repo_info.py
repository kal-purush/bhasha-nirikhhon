# import pandas as pd
# import requests
# import time
# import os
# import json
# from tqdm import tqdm

# # Load your data
# df = pd.read_csv("repo_language_classification1.csv")

# # GitHub token (set this in your environment or insert directly here)
# GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
# # print(GITHUB_TOKEN)
# headers = {
#     "Authorization": f"token {GITHUB_TOKEN}",
#     "Accept": "application/vnd.github+json"
# }

# def get_repo_data(repo_id):
#     # url = f"https://api.github.com/repos/{full_name}"
#     url = f"https://api.github.com/repositories/{repo_id}"
#     try:
#         response = requests.get(url, headers=headers)
#         print(response)
#         if response.status_code == 403 and "rate limit" in response.text.lower():
#             print("Rate limit hit. Sleeping for 60 seconds.")
#             time.sleep(60)
#             return get_repo_data(repo_id)
#         if response.status_code == 404:
#             return None
#         return response.json()
#     except Exception as e:
#         print(f"Error fetching {repo_id}: {e}")
#         return None

# languages = ['chinese', 'korean', 'japanese', 'english', 'mix', 'russian']
# all_results = {}

# for lang in tqdm(languages):
#     lang_df = df[(df['classification'] == lang) & (df['total_comments'] > 10)].reset_index(drop=True)

#     if len(lang_df) < 30:
#         print(f"Not enough repos for {lang}, found {len(lang_df)}")
#         continue

#     sampled_df = lang_df.sample(30, random_state=42).reset_index(drop=True)
#     lang_results = {}

#     for idx, row in sampled_df.iterrows():
#         repo_id = row.get('repo_id')
#         if not repo_id:
#             print(f"Missing repo_id for repo_id {row['repo_id']}")
#             continue

#         repo_data = get_repo_data(repo_id)
#         if repo_data:
#             print(f"Lang: {lang} | Repo: {repo_id}")
#             lang_results[repo_id] = repo_data
#         else:
#             print(f"Failed to fetch data for {repo_id}")

#     all_results[lang] = lang_results

# # Save to a JSON file
# with open("sampled_repos_metadata.json", "w") as f:
#     json.dump(all_results, f, indent=2)

# print("Saved all GitHub repo metadata to sampled_repos_metadata.json")


import pandas as pd
import requests
import time
import os
import json
from tqdm import tqdm
from datetime import datetime

# Load CSV
df = pd.read_csv("repo_language_classification1.csv")

# GitHub token
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

# Output folder
output_dir = "sampled_repo_metadata_individual"
os.makedirs(output_dir, exist_ok=True)

def get_repo_data(repo_id):
    url = f"https://api.github.com/repositories/{repo_id}"
    try:
        response = requests.get(url, headers=headers)

        # Check rate limit
        remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
        reset_time = int(response.headers.get("X-RateLimit-Reset", 0))

        if remaining == 0:
            reset_dt = datetime.utcfromtimestamp(reset_time)
            wait_time = reset_time - int(time.time()) + 5
            print(f"Rate limit hit. Waiting until {reset_dt} UTC ({wait_time} seconds)...")
            time.sleep(max(wait_time, 0))
            return get_repo_data(repo_id)

        if response.status_code == 404:
            return None

        if response.status_code != 200:
            print(f"Unexpected status {response.status_code} for {repo_id}")
            return None

        return response.json()

    except Exception as e:
        print(f"Error fetching {repo_id}: {e}")
        return None

# Filter non-English repos
lang_df = df[df['classification'] != 'english'].reset_index(drop=True)

# Loop through all rows
for idx, row in tqdm(lang_df.iterrows(), total=len(lang_df)):
    if idx<43000:
        continue
    repo_id = str(row.get('repo_id'))
    lang = row.get('classification', 'unknown')

    if not repo_id:
        continue

    out_path = os.path.join(output_dir, f"{lang}_{repo_id}.json")

    # Skip if already saved
    if os.path.exists(out_path):
        continue

    repo_data = get_repo_data(repo_id)
    if repo_data:
        with open(out_path, "w") as f:
            json.dump(repo_data, f, indent=2)
        print(f"Saved: {out_path}")
    else:
        print(f"Failed to fetch data for repo {repo_id}")

print("Done. All responses saved individually.")

