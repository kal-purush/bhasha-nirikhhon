from multiprocessing.pool import Pool
import os
import json
import re
import argparse
from collections import defaultdict
from lingua import Language, LanguageDetectorBuilder
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Argument parser for --field
parser = argparse.ArgumentParser(description="Analyze English vs non-English messages by month.")
parser.add_argument("--field", type=str, default="message", help="Field name to check language (default: 'message')")
parser.add_argument("--input_dir", type=str, default="parsed_json", help="Directory containing JSON files")
parser.add_argument("--output", type=str, default="language_stats_by_month.csv", help="Output CSV file")
args = parser.parse_args()

field_name = args.field
log_dir = args.input_dir
output_file = args.output

# Setup language detector
languages = [Language.ENGLISH, Language.SPANISH, Language.FRENCH, Language.GERMAN,
             Language.CHINESE, Language.JAPANESE, Language.HINDI, Language.BENGALI,
             Language.RUSSIAN, Language.PORTUGUESE, Language.ARABIC]
detector = LanguageDetectorBuilder.from_all_languages().build()

import re

def clean_body_text(text):
    if not text:
        return ""

    # Remove URLs
    text = re.sub(r'https?://\S+|www\.\S+', '', text)

    # Remove markdown/code blocks: inline and fenced
    text = re.sub(r'`{1,3}[^`]+`{1,3}', '', text)  # inline and code block
    text = re.sub(r'```[\s\S]+?```', '', text)     # fenced code block multiline

    # Remove emojis and other symbols
    text = re.sub(r'[^\p{L}\p{N}\s.,!?\'\"-]', '', text)

    return text.strip()

def is_english(text):
    if not text:
        return True
    for char in text:
        if char.isspace() or not char.isalpha():
            continue
        if not ('a' <= char.lower() <= 'z'):
            return False
    return True

def extract_month(date_str):
    match = re.search(r'\b([A-Z][a-z]{2})\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+(\d{4})\b', date_str)
    if match:
        month = match.group(1)
        year = match.group(2)
        month_num = {
            'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
            'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
            'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
        }.get(month)
        return f"{year}-{month_num}"
    return None

def default_stats():
    return {'non_english': 0, 'english': 0, 'total': 0}

def process_file(path):
    counts = defaultdict(default_stats)
    non_english_messages = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return counts, non_english_messages

    for entry in data:
        date = entry.get('date', '')
        text = entry.get(field_name, '')
        if isinstance(text, list):
            continue
        
        messages = text.split(',')

        month = extract_month(date)
        if not month:
            continue

        for message in messages:
            message = message.encode("utf-8", "ignore").decode("utf-8", "ignore")
            counts[month]['total'] += 1
            if is_english(message):
                lang = "unknown"
                if message.strip():
                    detection = detector.compute_language_confidence_values(message)
                    lang = detection[0].language.name.lower() if detection else "unknown"
                    if lang == "english":
                        counts[month]['english'] += 1
                    elif lang != "unknown":
                        if detection[0].value > 0.9:
                            counts[month]['non_english'] += 1
                            non_english_messages.append({
                                "date": date,
                                "message": message,
                                "detected_lang": lang,
                                "confidence": detection[0].value
                            })
            else:
                counts[month]['non_english'] += 1
                non_english_messages.append({
                    "date": date,
                    "message": message,
                    "detected_lang": "non_english",
                    "confidence": None
                })
    return counts, non_english_messages

def merge_counts(results):
    final = defaultdict(lambda: {'non_english': 0, 'english': 0, 'total': 0})
    for result in results:
        for month, values in result.items():
            final[month]['english'] += values['english']
            final[month]['non_english'] += values['non_english']
            final[month]['total'] += values['total']
    return final

def process_file_safe(filepath):
    try:
        return process_file(filepath)
    except Exception as e:
        # Log the error and return default empty values
        print(f"Error processing {filepath}: {e}")
        return ({}, [])  # Or whatever default makes sense


def main():
    # Collect JSON files
    json_files = [os.path.join(log_dir, f) for f in os.listdir(log_dir) if f.endswith(".json")]
    json_files = json_files[:1000]  # Limit to first 100 files for testing
    max_threads = 8  # You can change this number
    # Run processing with ThreadPoolExecutor
    all_results = []
    all_non_english = []

    # with ThreadPoolExecutor(max_workers=max_threads) as executor:
    #     futures = [executor.submit(process_file, path) for path in json_files]
    #     for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files"):
    #         counts, non_english_msgs = future.result()
    #         all_results.append(counts)
    #         all_non_english.extend(non_english_msgs)
    with Pool(processes=max_threads) as pool:
        results = list(tqdm(pool.imap(process_file_safe, json_files), total=len(json_files), desc="Processing files"))
        for counts, non_english_msgs in results:
            all_results.append(counts)
            all_non_english.extend(non_english_msgs)

    # Merge and write results
    final_counts = merge_counts(all_results)

    with open(output_file, "w", encoding="utf-8") as out:
        out.write("month,english,non_english,total,percent_english\n")
        for month in sorted(final_counts):
            english = final_counts[month]['english']
            non_english = final_counts[month]['non_english']
            total = final_counts[month]['total']
            percent = (english / total * 100) if total else 0
            out.write(f"{month},{english},{non_english},{total},{percent:.2f}\n")

    # Save non-English messages
    non_english_file = output_file.replace('.csv', '_non_english.json')
    with open(non_english_file, "w", encoding="utf-8") as f:
        json.dump(all_non_english, f, ensure_ascii=False, indent=2)

    print(f"\nSaved results to {output_file}")
    print(f"Saved non-English messages to {non_english_file}")

if __name__ == '__main__':
    main()