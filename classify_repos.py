# from multiprocessing import Pool, cpu_count
# from collections import defaultdict
# from tqdm import tqdm
# import pandas as pd
# import os

# comment_counts = defaultdict(lambda: defaultdict(int))
# years = list(range(2015, 2026))
# months = range(1, 13)

# def process_push_file(args):
#     year, month = args
#     file_name = f"interactions_files_new/comment_count_{year}_{month:02d}_all_days.csv"
#     local_counts = []

#     if not os.path.exists(file_name):
#         return []

#     try:
#         df = pd.read_csv(file_name)
#         for _, row in df.iterrows():
#             repo_id = row['repo_id']
#             language = row['language']
#             count = row['comment_count']
#             local_counts.append((repo_id, language, count))
#     except Exception as e:
#         print(f"Error reading {file_name}: {e}")

#     return local_counts

# if __name__ == "__main__":
#     tasks = [(year, month) for year in years for month in months]

#     with Pool(processes=min(cpu_count(), 24)) as pool:
#         for result in tqdm(pool.imap_unordered(process_push_file, tasks), total=len(tasks), desc="Processing months"):
#             for repo_id, language, count in result:
#                 comment_counts[repo_id][language] += count

#     # # Optional: print summary
#     # for repo_id in list(comment_counts):
#     #     print(f"{repo_id}: {dict(comment_counts[repo_id])}")
#     # Step 1: Filter repos with multiple languages and calculate total comments
#     multi_lang_repos = {}

#     for repo_id, lang_counts in comment_counts.items():
#         if len(lang_counts) > 1:
#             total_comments = sum(lang_counts.values())
#             multi_lang_repos[repo_id] = total_comments

#     # Step 2: Sort repos by total comment count in descending order
#     top_10_repos = sorted(multi_lang_repos.items(), key=lambda x: x[1], reverse=True)[:10]

#     # Step 3: Print the results
#     print("\nTop 10 repos with multiple languages and highest comment counts:")
#     for repo_id, total in top_10_repos:
#         print(f"{repo_id}: {total} comments")

#     # Flatten comment_counts into a list of dictionaries
#     flattened_data = []

#     for repo_id, lang_counts in comment_counts.items():
#         for language, count in lang_counts.items():
#             flattened_data.append({
#                 "repo_id": repo_id,
#                 "language": language,
#                 "comment_count": count
#             })

#     # Convert to DataFrame and save to CSV
#     df_output = pd.DataFrame(flattened_data)
#     df_output.to_csv("all_repo_comment_counts.csv", index=False)

#     print("Saved comment counts to all_repo_comment_counts.csv")


import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# Load the data
df = pd.read_csv("all_repo_comment_counts.csv")

# Group by repo_id
grouped = df.groupby("repo_id")
repo_data = [(repo_id, group) for repo_id, group in grouped]

# Classification function
def classify_repo(args):
    repo_id, group = args
    lang_counts = group.set_index("language")["comment_count"].to_dict()
    total_comments = sum(lang_counts.values())
    english_comments = lang_counts.get("english", 0)
    english_ratio = english_comments / total_comments if total_comments > 0 else 0

    if english_ratio >= 0.9:
        repo_class = "english"
    else:
        repo_class = "mixed"
        for lang, count in lang_counts.items():
            if lang != "english" and (count / total_comments) >= 0.9:
                repo_class = lang
                break

    return {
        "repo_id": repo_id,
        "total_comments": total_comments,
        "classification": repo_class
    }

if __name__ == "__main__":
    with Pool(processes=min(cpu_count(), 24)) as pool:
        results = list(tqdm(pool.imap_unordered(classify_repo, repo_data), total=len(repo_data), desc="Classifying repos"))

    df_classified = pd.DataFrame(results)
    df_classified.to_csv("repo_language_classification1.csv", index=False)

    print("Saved repo classifications to repo_language_classification.csv")

