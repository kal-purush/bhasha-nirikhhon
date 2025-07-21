results = []
import asyncio
import os
from googletrans import Translator
import json

import re
import pandas as pd

# Read content from file
with open("lang_detection_result_merged.txt", "r", encoding="utf-8") as f:
    lines = f.readlines()

segments = []
current_lines = []

# Regex to detect end of a full segment
end_pattern = re.compile(r'->\s*([\w\-]+),\s*([\d.]+)\s*$')
count=0
for line in lines:
    current_lines.append(line.rstrip())

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
symbols_with_lang = dict(zip(df['text'], df['lang']))

json_folder = "language_detection_results_rq4"
translator = Translator()

async def detect_languages(text):
    async with Translator() as translator:
        translations = await translator.detect(text)
        return translations
for year in (2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025):
# for year in (2025,):
    file_path = os.path.join(json_folder, f"non_english_{year}.json")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            print(file_path)
            data = json.load(f)
            # for key in ['classes', 'identifiers', 'variables', 'functions']:
            # for key in ['literals']:
            for key in ['comments', 'docstrings']:
                if key in data:
                    for file_key, text in data[key].items():
                        try:
                            for text_snippet in text:
                                if text_snippet not in symbols_with_lang:
                                    translations = asyncio.run(detect_languages(text_snippet))
                                    print(f"{text_snippet} -> {translations}")
                                # for i in range(len(translations)):
                                #     print(f"{text[i]} -> {translations[i]}")
                                #     results.append({
                                #         "year": year,
                                #         "key": key,
                                #         "file_key": file_key,
                                #         "original": text[i],
                                #         "detected": translations[i]
                                #     })
                        except:
                            pass
        # # Save results to a file
        with open(f"translation_detection_results_{year}.json", "w", encoding="utf-8") as out_f:
            json.dump(results, out_f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

