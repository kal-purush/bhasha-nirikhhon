import os
import gzip
import json
import ssl
from collections import defaultdict
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import argparse
from lingua import Language, LanguageDetectorBuilder
from functools import partial
import regex as re 
# Disable SSL validation if needed
ssl._create_default_https_context = ssl._create_unverified_context

year = 2015
month = 1
day = 1
download_dir = f"data/gharchive_{year}_{month:02d}"
day_str = f"{year}-{month:02d}-{day:02d}"

def process_hour_mp(hour, year, month, day, download_dir, word_list):
    day_str = f"{year}-{month:02d}-{day:02d}"
    filename = f"{day_str}-{hour}.json.gz"
    filepath = os.path.join(download_dir, filename)

    found_words = set()
    not_found_words = set(word_list)

    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    # print(line[:100])
                    for word in word_list:
                        if word in found_words:
                            continue
                        if word in line:
                            found_words.add(word)
                            not_found_words.discard(word)
                except Exception:
                    raise
    except Exception:
        raise
    return list(found_words), list(not_found_words)

# if __name__ == "__main__":
from multiprocessing import Pool, cpu_count
from collections import defaultdict
from functools import partial
from tqdm import tqdm
if __name__ == "__main__":
    with open("hash_dict.json", "r") as f:
        data = json.load(f)
    word_list = data[f"{year}-{month:02d}"]  # Convert to lowercase for case-insensitive matching
    print(len(word_list), "words to search for")

    found_words_total = set()
    remaining_words = set(word_list)

    with Pool(processes=cpu_count()) as pool:
        func = partial(process_hour_mp, year=year, month=month, day=day,
                        download_dir=download_dir, word_list=word_list)
        for result in tqdm(pool.imap_unordered(func, range(24)), total=24, desc=f"Day {day:02d}"):
            if result is None:
                continue
            found_words, _ = result
            found_words_total.update(found_words)
            remaining_words.difference_update(found_words)
            if not remaining_words:
                break  # all words found, stop early

    print("Found words:", len(found_words_total))
    print("Not found words:", len(remaining_words))
