import os
import glob
import json
import re
import pandas as pd
import matplotlib.pyplot as plt
import random
from collections import defaultdict
from pathlib import Path

import re
import pandas as pd
files = glob.glob("./lang_detection_result*")
segments = []

for file_ in files:
    # if "lang_detection_result_merged.txt" in file_:
        # continue
    # Read content from file
    with open(file_, "r", encoding="utf-8") as f:
        lines = f.readlines()
    current_lines = []
    # Regex to detect end of a full segment
    end_pattern = re.compile(r'->\s*([\w\-]+),\s*([\d.]+)\s*$')
    count=0
    for line in lines:
        current_lines.append(line)

        # Check if this line ends a segment
        match = end_pattern.search(line)
        if match:
            lang = match.group(1)
            confidence = float(match.group(2))
            
            # Get the last line and remove the arrow part
            last_line = current_lines[-1]
            text_before_arrow = last_line.split('->')[0].rstrip()
            
            # Join all lines, replacing the last one with the trimmed version
            full_text = ''.join(current_lines[:-1] + [text_before_arrow]).strip()

            # if "上午11:48:17" in full_text:
            #     print(full_text)

            # Exclude if text starts with any year in [2015]...[2025]
            if not any(full_text.startswith(f"[{y}]") for y in range(2015, 2026)):
                if "Created by wvv8oo on 1/26/15." in full_text:
                    print(full_text)
                segments.append({
                    "text": full_text,
                    "lang": lang,
                    "confidence": confidence
                })

            # Reset buffer
            current_lines = []
        else:
            # print(line)
            count+=1

df = pd.DataFrame(segments)
df = df.drop_duplicates(keep='first').reset_index(drop=True)
df = df[df["confidence"]>=0.9]
symbols_with_lang = dict(zip(df['text'], df['lang']))

# Replace this with your actual mapping
# symbols_with_lang = {}  # Example: {"esto es español": "spanish", "hello": "english"}

keys = ['comments', 'classes', 'identifiers', 'functions', 'literals']
# keys = ['comments']
years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
# years = [2019, 2020, 2021, 2023, 2024, 2025]

target_folder = 'rq4_logs_new'
sub_folders = glob.glob(f"{target_folder}/*")
year_months = set()
file_ext_dict={}
# for ext in ['.py', '.java', '.js', '.ts', '.cs']:
for folder in sub_folders:
    files = glob.glob(f"{folder}/*")
    # print(folder, len(files))
    for file_ in files:
        ext = Path(file_).suffix.lower()
        
        folder_name = os.path.basename(folder)
        parts = folder_name.split('-')
        if len(parts) >= 2:
            year, month = parts[0], parts[1]
        else:
            year, month = "unknown", "unknown"

        base_name = os.path.basename(file_)
        json_name = f"{os.path.splitext(base_name)[0]}_{year}_{month}.json"
        out_dir = f"code_parser_data_rq4/{year}-{month}"
        out_path = os.path.join(out_dir, json_name)
        file_ext_dict[out_path] = ext
        # print(file_, base_name, json_name, out_path, ext)


months = range(1, 13)

def is_english(text):
    if not text:
        return True
    for char in text:
        if char.isspace() or not char.isalpha():
            continue
        if not ('a' <= char.lower() <= 'z'):
            return False
    return True

def get_language_from_filename(filename):
    ext = file_ext_dict[filename]
    mapping = {
        ".py": "Python",
        ".js": "JavaScript",
        ".java": "Java",
        ".cpp": "C++",
        ".c": "C",
        ".cs": "C#",
        ".ts": "TypeScript",
        ".rb": "Ruby",
        ".go": "Go",
        ".php": "PHP",
        ".rs": "Rust",
        ".kt": "Kotlin",
        ".swift": "Swift"
    }
    return mapping.get(ext, "Other")

all_records = []
all_dfs= []
for key in keys:
    print(f"FOR {key} =====================>")
    records = []

    for year in years:
        for month in months:
            log_dir = f"code_parser_data_rq4/{year}-{month:02d}"
            files = glob.glob(f"{log_dir}/*")

            # group files by language
            lang_file_map = defaultdict(list)
            for file_ in files:
                lang = get_language_from_filename(file_)
                lang_file_map[lang].append(file_)

            # now sample per language
            sampled_files = []
            for lang, lang_files in lang_file_map.items():
                if len(lang_files) > 100:
                    lang_files = random.sample(lang_files, 100)
                sampled_files.extend(lang_files)

            stats = defaultdict(lambda: {"total": 0, "non_english": 0})
            # print(len(sampled_files))
            for file_ in sampled_files:
                try:
                    with open(file_) as in_file:
                        data = json.load(in_file)
                        if key == 'identifiers':
                            values = data.get('identifiers', []) + data.get('variables', [])
                        elif key == 'comments':
                            values = data.get('docstrings', []) + data.get('comments', [])
                        else:
                            values = data.get(key, [])
                        if not values:
                            continue

                        language = get_language_from_filename(file_)
                        has_non_english = False

                        for item in values:
                            if not is_english(item):
                                lang = symbols_with_lang.get(item)
                                if not lang or lang.lower() != "en":
                                    has_non_english = True
                                    break

                        stats[language]["total"] += 1
                        if has_non_english:
                            # if language == "Java":
                            #     print(file_)
                            stats[language]["non_english"] += 1
                except:
                    continue

            for lang, counts in stats.items():
                records.append({
                    "key": key,
                    "year": year,
                    "month": month,
                    "language": lang,
                    "total_files": counts["total"],
                    "non_english_files": counts["non_english"]
                })

    df = pd.DataFrame(records)
    if df.empty:
        continue

    # df["perc_non_english"] = df["non_english_files"] / df["total_files"].replace(0, pd.NA) * 100
    df["perc_non_english"] = (df["non_english_files"] / 100) * 100

    # Group by year and language, then average
    yearly_df = df.groupby(["year", "language"])["perc_non_english"].mean().reset_index()

    # Pivot for plotting
    pivot_df = yearly_df.pivot(index="year", columns="language", values="perc_non_english").fillna(0)
    all_dfs.append(pivot_df)

years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
# years = [15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]
keys = ['comments', 'classes', 'identifiers', 'functions', 'literals']
language_order = ['Python', 'Java', 'JavaScript', 'C#', 'TypeScript']

for i in range(len(all_dfs)):
    key = keys[i]
    pivot_df = all_dfs[i]
    # Only plot the specified language order, if present in the DataFrame
    langs_to_plot = [lang for lang in language_order if lang in pivot_df.columns]
    pivot_df[langs_to_plot].plot(kind="line", figsize=(6, 3.5), marker='o')

    plt.ylabel("Percent of \nNon-English Files", fontsize=16)
    plt.xlabel("")  # Use empty string instead of None for type safety
    # plt.title(f"Non-English Code Content by Language: {key}")
    plt.xticks(years, [str(y)[-2:] for y in years], fontsize=18, rotation=0)
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x)}"))
    plt.yticks(fontsize=18)
    plt.legend(ncol=3, bbox_to_anchor=(.5, 1.4), loc='upper center', fontsize=14)
    plt.tight_layout()
    plt.savefig(f"non_english_by_language_{key}.pdf", bbox_inches='tight')
    plt.show()
   