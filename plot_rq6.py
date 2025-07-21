# from multiprocessing import Pool, cpu_count
# from collections import defaultdict
# import pandas as pd
# from tqdm import tqdm
# import matplotlib.pyplot as plt
# import os

# years = list(range(2015, 2026))
# months = list(range(1, 13))
# tasks = [(y, m) for y in years for m in months]

# # ---------------------- Push Count Function ----------------------
# def load_push_file(args):
#     year, month = args
#     fname = f"interactions_files_new/push_count_{year}_{month:02d}_all_days.csv"
#     local = defaultdict(int)
#     if not os.path.exists(fname):
#         return local
#     try:
#         # Read the file line by line, skipping the header
#         with open(fname, 'r', encoding='utf-8') as f:
#             header = f.readline().strip().split(',')
#             repo_id_idx = header.index('repo_id')
#             push_count_idx = header.index('push_count')
#             for line in f:
#                 parts = line.strip().split(',')
#                 if len(parts) < max(repo_id_idx, push_count_idx) + 1:
#                     continue
#                 try:
#                     repo_id = int(parts[repo_id_idx])
#                     push_count = int(parts[push_count_idx])
#                     local[repo_id] += push_count
#                 except Exception as e:
#                     print(f"Error parsing line in {fname}: {e}")
#     except Exception as e:
#         print(f"Error reading {fname}: {e}")
#     return local

# # # ---------------------- Comment Count Function ----------------------
# # def load_comment_file(args):
# #     year, month = args
# #     fname = f"interactions_files_new/comment_count_{year}_{month:02d}_all_days.csv"
# #     comment_data = []
# #     if not os.path.exists(fname):
# #         return comment_data
# #     try:
# #         df = pd.read_csv(fname)
# #         for _, row in df.iterrows():
# #             comment_data.append((
# #                 row['repo_id'],
# #                 row['issue_id'],
# #                 row['language'],
# #                 row.get('html_url', None)
# #             ))
# #     except Exception as e:
# #         print(f"Error reading {fname}: {e}")
# #     return comment_data

# if __name__ == '__main__':
#     push_counts = defaultdict(int)
#     comment_counts = defaultdict(int)
#     issue_languages = defaultdict(set)
#     repo_files = defaultdict(list)
#     repo_is_multilingual = defaultdict(bool)
#     df = pd.read_csv('repo_language_classification1.csv')
#     repo_ids = df['repo_id'].tolist()
#     # ------------ Process Push Files in Parallel ------------
#     print("done")
#     with Pool(processes=min(cpu_count(), 24)) as pool:
#         for local_result in tqdm(pool.imap_unordered(load_push_file, tasks), total=len(tasks), desc="Push files"):
#             for repo_id, count in local_result.items():
#                 if repo_id in repo_ids:
#                     push_counts[repo_id] += count

#     # save push_counts to csv
#     t_df = pd.DataFrame(push_counts.items(), columns=['repo_id', 'push_count'])
#     t_df.to_csv("push_counts_valid.csv", index=False)

    # # ------------ Process Comment Files in Parallel ------------
    # with Pool(processes=min(cpu_count(), 24)) as pool:
    #     for comment_result in tqdm(pool.imap_unordered(load_comment_file, tasks), total=len(tasks), desc="Comment files"):
    #         for repo_id, issue_id, language, html_url in comment_result:
    #             comment_counts[repo_id] += 1
    #             issue_languages[(repo_id, issue_id)].add(language)
    #             if html_url:
    #                 repo_files[repo_id].append(html_url)

    #             if repo_id == 724712 and "chinese" in language:
    #                 print(f"Found specific issue: {repo_id}, {issue_id} with language {html_url}")

    # # ------------ Detect Multilingual Issues ------------
    # for (repo_id, issue_id), langs in issue_languages.items():
    #     if len(langs) > 1:
    #         # print(f"Multilingual issue found: {repo_id}, {issue_id} with languages {langs}")
    #         repo_is_multilingual[repo_id] = True

    # print("MULTI DONE")
    # # ------------ Prepare Plot Data ------------
    # # all_repos = set(push_counts) | set(comment_counts)
    # plot_data = []

    # # for repo_id in tqdm(len(all_repos)):
    # for repo_id in tqdm(push_counts.keys()):
    #     plot_data.append({
    #         'repo_id': repo_id,
    #         'push_count': push_counts.get(repo_id, 0),
    #         'comment_count': comment_counts.get(repo_id, 0),
    #         'is_multilingual': repo_is_multilingual.get(repo_id, False)
    #         # 'html_url': repo_files.get(repo_id, [])
    #     })
    # print(len(push_counts.keys()))
    # print("Done getting data!")
    # df_plot = pd.DataFrame(plot_data)
    # df_plot.to_csv("Multi_repo.csv", index=False)
    # print("Done getting data!")
    # # # ------------ Plot ------------
    # # plt.figure(figsize=(10, 6))
    # # multi = df_plot[df_plot['is_multilingual']]
    # # non_multi = df_plot[~df_plot['is_multilingual']]

    # # plt.scatter(multi['push_count'], multi['comment_count'], alpha=0.5, label='Multilingual', s=20)
    # # plt.scatter(non_multi['push_count'], non_multi['comment_count'], alpha=0.5, label='Non-Multilingual', s=20)

    # # plt.xlabel('Push Count (per Repo)')
    # # plt.ylabel('Comment Count (per Repo)')
    # # plt.title('Repo Push Count vs Issue Comment Count')
    # # plt.legend()
    # # plt.xscale('log')
    # # plt.grid(True)
    # # plt.tight_layout()
    # # plt.show()


# from multiprocessing import Pool, cpu_count
# from collections import defaultdict
# import pandas as pd
# from tqdm import tqdm
# import matplotlib.pyplot as plt
# import os

# years = list(range(2015, 2026))
# months = list(range(1, 13))
# tasks = [(y, m) for y in years for m in months]

# # ---------------------- Push Count Function ----------------------
# def load_push_file(year, month, repo_id):
#     # year, month = args
#     fname = f"interactions_files_new/push_count_{year}_{month:02d}_all_days.csv"
#     local = defaultdict(int)
#     if not os.path.exists(fname):
#         return local
#     try:
#         # Read the file line by line, skipping the header
#         with open(fname, 'r', encoding='utf-8') as f:
#             for line in f:
#                 try:
#                     if "repo_id,push_count" not in line:
#                         parts = line.split(",")
#                         repo_id = int(parts[0])
#                         push_count = int(parts[1])
#                         local[repo_id] += push_count
#                 except Exception as e:
#                     print(f"Error parsing line in {fname}: {e}")
#     except Exception as e:
#         print(f"Error reading {fname}: {e}")
#     return local


# if __name__ == '__main__':
#     push_counts = defaultdict(int)
#     comment_counts = defaultdict(int)
#     issue_languages = defaultdict(set)
#     repo_files = defaultdict(list)
#     repo_is_multilingual = defaultdict(bool)
#     df = pd.read_csv('repo_language_classification1.csv')
#     repo_ids = df['repo_id'].tolist()
#     # ------------ Process Push Files in Parallel ------------
#     print("done")
#     for year in years:
#         for month in months:
#             print(f"loading.......{year}-{month}")
#             local_result = load_push_file(year, month, repo_ids)
#             for repo_id, count in local_result.items():
#                 if repo_id in repo_ids:
#                     push_counts[repo_id] += count
#     # save push_counts to csv
#     t_df = pd.DataFrame(push_counts.items(), columns=['repo_id', 'push_count'])
#     t_df.to_csv("push_counts_valid.csv", index=False)


import sys
from collections import defaultdict
import pandas as pd
import os

# ---------------------- Push Count Function ----------------------
def load_push_file(year, month, repo_ids):
    fname = f"interactions_files_new/push_count_{year}_{month:02d}_all_days.csv"
    local = defaultdict(int)
    if not os.path.exists(fname):
        print(f"File not found: {fname}")
        return local
    try:
        with open(fname, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    if "repo_id,push_count" not in line:
                        parts = line.strip().split(",")
                        repo_id = int(parts[0])
                        push_count = int(parts[1])
                        if repo_id in repo_ids:
                            local[repo_id] += push_count
                except Exception as e:
                    print(f"Error parsing line in {fname}: {e}")
    except Exception as e:
        print(f"Error reading {fname}: {e}")
    return local

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python script.py <year> <month>")
        sys.exit(1)

    year = int(sys.argv[1])
    month = int(sys.argv[2])

    df = pd.read_csv('repo_language_classification1.csv')
    repo_ids = set(df['repo_id'].tolist())

    print(f"Processing push counts for {year}-{month:02d}...")
    push_counts = load_push_file(year, month, repo_ids)

    # Save push counts to file
    t_df = pd.DataFrame(push_counts.items(), columns=['repo_id', 'push_count'])
    output_path = f"push_counts_valid_{year}_{month:02d}.csv"
    t_df.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")
