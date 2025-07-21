import pandas as pd
from tqdm import tqdm
import multiprocessing

# Load once in main process
df = pd.read_csv('repo_language_classification1.csv')
repo_ids = df['repo_id'].tolist()

push_df = pd.read_csv("push_counts.csv")

# Global variable for workers
shared_push_df = None

def init_worker(df_shared):
    global shared_push_df
    shared_push_df = df_shared

def process_repo(repo_id):
    repo_pushes = shared_push_df[shared_push_df['repo_id'] == repo_id].reset_index(drop=True)
    if len(repo_pushes) != 0:
        push_count = repo_pushes.iloc[0]['push_count']
        if len(repo_pushes) > 1:
            print(f"PROBLEM for repo_id {repo_id} !!!!!!!!!!!!!!!!!!!!!!!")
        return (repo_id, push_count)
    return None

if __name__ == '__main__':
    # Pass the DataFrame as a copy to each worker via initializer
    # DataFrames aren't natively shareable, so each process gets its own copy
    with multiprocessing.Pool(initializer=init_worker, initargs=(push_df,)) as pool:
        results = []
        for result in tqdm(pool.imap_unordered(process_repo, repo_ids), total=len(repo_ids)):
            if result:
                results.append(result)

    # Save result
    result_df = pd.DataFrame(results, columns=['repo_id', 'push_count'])
    result_df.to_csv('valid_repos.csv', index=False)
