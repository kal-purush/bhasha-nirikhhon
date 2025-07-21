import argparse
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import os
import gzip
import json
from collections import defaultdict
import time
import pandas as pd
import regex as re
import unicodedata
# import tiktoken
# from openai import OpenAI
from multiprocessing import Pool, cpu_count
from functools import partial
from tqdm import tqdm
from lingua import Language, LanguageDetectorBuilder

parser = argparse.ArgumentParser(description="Process GH Archive event counts and language stats.")
parser.add_argument("--year", type=int, required=True)
# parser.add_argument("--month", type=int, required=True)
# parser.add_argument("--day", type=int, required=True)
args = parser.parse_args()

year = args.year

# for month in range(1, 13):
#     for day in range(1, 32):
#         try:
#             file_name = f"resul_files/messages_with_languages_{year}_{month:02d}_{day:02d}.csv"
#             if not os.path.exists(file_name):
#                 print(f"File not found: {file_name}")
#                 continue
            
#             df = pd.read_csv(file_name)
#             df = df.drop_duplicates(keep='first').reset_index(drop=True)
#             # count the number of messages for each language
#             language_counts = df['language'].value_counts().reset_index()
#             language_counts.columns = ['language', 'count']
#             language_counts['date'] = f"{year}-{month:02d}-{day:02d}"
#             # print(f"Processing {file_name} with {len(df)} messages.")
#             # print(f"Language counts:\n{language_counts}")
            
#             break
#         except Exception as e:
#             print(f"Error processing {file_name}: {e}")
#     break

records = []

def process_day_file(args):
    year, month, day = args
    file_name = f"resul_files/messages_with_languages_{year}_{month:02d}_{day:02d}.csv"
    if not os.path.exists(file_name):
        return None
    try:
        df = pd.read_csv(file_name, on_bad_lines='skip')
        df = df.drop_duplicates(keep='first')
        return df['language'].value_counts().to_dict()
    except Exception as e:
        print(f"Error processing {file_name}: {e}")
        return None

if __name__ == '__main__':
    records = []

    for month in range(1, 13):
        monthly_counts = defaultdict(int)
        tasks = [(year, month, day) for day in range(1, 32)]

        with multiprocessing.Pool(processes=os.cpu_count()) as pool:
            for result in tqdm(pool.imap_unordered(process_day_file, tasks), total=31, desc=f"Month {month:02d}"):
                if result:
                    for lang, count in result.items():
                        monthly_counts[lang] += count

        for lang, count in monthly_counts.items():
            records.append({
                'language': lang,
                'count': count,
                'date': f"{year}-{month:02d}"
            })

    result_df = pd.DataFrame(records)
    result_df.to_csv(f"language_counts_by_month_{year}.csv", index=False)