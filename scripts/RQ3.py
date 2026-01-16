import glob
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_palette("Set2")

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

# keys to analyze
keys = ['comments', 'docstrings', 'classes', 'identifiers', 'functions', 'literals']
# keys = ['docstrings']
years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
months = range(1, 13)

# This should be defined with real language detection results
# symbols_with_lang = {}  # replace with your language map

def is_english(text):
    if not text:
        return True
    for char in text:
        if char.isspace() or not char.isalpha():
            continue
        if not ('a' <= char.lower() <= 'z'):
            return False
    return True

records = []
# not_found = 0
for key in keys:
    print(f"FOR {key} =====================>")
    for year in years:
        not_found = 0
        total = 0
        full_english = 0
        for month in months:
            log_dir = f"code_parser_data_rq3/{year}-{month:02d}"
            files = glob.glob(f"{log_dir}/*")
            total_files = 0
            non_english_files = 0

            for file_ in files:
                try:
                    with open(file_) as in_file:
                        data = json.load(in_file)
                        if key == 'identifiers':
                            values = data.get('identifiers', []) + data.get('variables', [])
                        elif key == 'comments':
                            values = data.get('docstrings', []) + data.get('comments', [])
                        else:
                            values = data.get(key, [])
                        # values = data.get(key, [])
                        if not values:
                            continue

                        total_files += 1
                        has_non_english = False
                        # print(values)
                        for item in values:
                            total += 1
                            if not is_english(item):
                                try:
                                    lang = symbols_with_lang.get(item.strip())
                                    # print(lang)
                                    # if lang:
                                    if lang.lower() != "en":
                                        has_non_english = True
                                        # break
                                except:
                                    # print(item)
                                    # print("======================", year)
                                    not_found+=1
                                    # raise
                            else:
                                full_english += 1
                        # print(has_non_english)
                        if has_non_english:
                            non_english_files += 1
                except Exception:
                    raise

            records.append({
                "key": key,
                "year": year,
                "month": month,
                "total_files": total_files,
                "non_english_files": non_english_files
            })
        denominator = total - full_english
        percent_not_found = (not_found / denominator) * 100 if denominator != 0 else 0
        print(year, not_found, full_english, total, denominator, percent_not_found)


# Group by year and key, sum the files
df = pd.DataFrame(records)
df["year_month"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
df["perc_non_english_files"] = (df["non_english_files"] / 385) * 100
df_yearly = df.groupby(['key', 'year']).agg({
    'total_files': 'sum',
    'non_english_files': 'sum'
}).reset_index()
df_yearly['perc_non_english_files'] = df_yearly['non_english_files'] / df_yearly['total_files'].replace(0, pd.NA) * 100

# keys_to_plot = ['comments', 'docstrings', 'classes', 'identifiers', 'functions', 'literals']
keys_to_plot = ['comments', 'classes', 'identifiers', 'functions', 'literals']

# Create a combined plot
plt.figure(figsize=(10, 4))
for key in keys_to_plot:
    subset = df_yearly[df_yearly["key"] == key]
    sns.lineplot(data=subset, x="year", y="perc_non_english_files", lw=3, marker='o', label=key)
plt.xticks(subset["year"])
plt.ylabel("% Non-English Files", fontsize=18)
plt.xlabel(None)
plt.yticks(fontsize=18)
plt.xticks(fontsize=18)
# plt.xlabel("Year", fontsize=20)
# plt.title(f"Non-English File Percentage Over Years - {key}")
# plt.legend(ncol=2, fontsize=16)
plt.legend(ncol=3, bbox_to_anchor=(.5, 1.22), loc="center", fontsize=20)
plt.tight_layout()
plt.savefig(f"combined_non_english_trend.pdf",bbox_inches='tight')
plt.show()
