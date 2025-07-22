import argparse
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
parser.add_argument("--month", type=int, required=True)
# parser.add_argument("--day", type=int, required=True)
args = parser.parse_args()

year = args.year
month = args.month
def get_unicode_and_script(text):
    # Comprehensive list of Unicode block ranges and their associated scripts
    # Covers major blocks from Unicode 15.1; includes most scripts and some symbol blocks
    unicode_blocks = [
        ((0x0000, 0x007F), "Latin (Basic Latin)", "Basic Latin alphabet used in English and many Western languages"),
        ((0x0080, 0x00FF), "Latin (Latin-1 Supplement)", "Extended Latin characters for Western European languages"),
        ((0x0100, 0x017F), "Latin (Latin Extended-A)", "Additional Latin characters for European languages like Czech, Polish"),
        ((0x0180, 0x024F), "Latin (Latin Extended-B)", "Further Latin extensions for African and other languages"),
        ((0x0250, 0x02AF), "IPA Extensions", "Phonetic symbols for International Phonetic Alphabet"),
        ((0x02B0, 0x02FF), "Spacing Modifier Letters", "Modifiers for phonetic transcription and tone"),
        ((0x0300, 0x036F), "Combining Diacritical Marks", "Diacritics combining with base characters for accents"),
        ((0x0370, 0x03FF), "Greek and Coptic", "Greek alphabet and Coptic script for Greek and Coptic languages"),
        ((0x0400, 0x04FF), "Cyrillic", "Alphabet for Russian, Ukrainian, and other Slavic languages"),
        ((0x0500, 0x052F), "Cyrillic Supplement", "Additional Cyrillic characters for minority languages"),
        ((0x0530, 0x058F), "Armenian", "Armenian alphabet for the Armenian language"),
        ((0x0590, 0x05FF), "Hebrew", "Hebrew script for Hebrew and Yiddish"),
        ((0x0600, 0x06FF), "Arabic", "Arabic script for Arabic, Persian, and Urdu"),
        ((0x0700, 0x074F), "Syriac", "Syriac script for Aramaic and Syriac languages"),
        ((0x0750, 0x077F), "Arabic Supplement", "Additional Arabic characters for specific languages"),
        ((0x0780, 0x07BF), "Thaana", "Thaana script for Dhivehi (Maldivian language)"),
        ((0x07C0, 0x07FF), "NKo", "N'Ko script for Manding languages in West Africa"),
        ((0x0800, 0x083F), "Samaritan", "Samaritan script for the Samaritan language"),
        ((0x0840, 0x085F), "Mandaic", "Mandaic script for the Mandaic language"),
        ((0x0860, 0x086F), "Syriac Supplement", "Additional Syriac characters"),
        ((0x08A0, 0x08FF), "Arabic Extended-A", "Extended Arabic characters for specific dialects"),
        ((0x0900, 0x097F), "Devanagari", "Devanagari script for Hindi, Marathi, and Sanskrit"),
        ((0x0980, 0x09FF), "Bengali", "Bengali script for Bengali and Assamese"),
        ((0x0A00, 0x0A7F), "Gurmukhi", "Gurmukhi script for Punjabi"),
        ((0x0A80, 0x0AFF), "Gujarati", "Gujarati script for the Gujarati language"),
        ((0x0B00, 0x0B7F), "Oriya", "Odia script for the Odia language"),
        ((0x0B80, 0x0BFF), "Tamil", "Tamil script for the Tamil language"),
        ((0x0C00, 0x0C7F), "Telugu", "Telugu script for the Telugu language"),
        ((0x0C80, 0x0CFF), "Kannada", "Kannada script for the Kannada language"),
        ((0x0D00, 0x0D7F), "Malayalam", "Malayalam script for the Malayalam language"),
        ((0x0D80, 0x0DFF), "Sinhala", "Sinhala script for the Sinhala language"),
        ((0x0E00, 0x0E7F), "Thai", "Thai script for the Thai language"),
        ((0x0E80, 0x0EFF), "Lao", "Lao script for the Lao language"),
        ((0x0F00, 0x0FFF), "Tibetan", "Tibetan script for Tibetan and Dzongkha"),
        ((0x1000, 0x109F), "Myanmar", "Myanmar script for Burmese and other languages"),
        ((0x10A0, 0x10FF), "Georgian", "Georgian script for the Georgian language"),
        ((0x1100, 0xD7FF), "CJK Combined (Chinese, Japanese, Korean)", "Combined CJK scripts including: Hangul Jamo (U+1100–U+11FF), CJK Radicals Supplement (U+2E80–U+2EFF), Kangxi Radicals (U+2F00–U+2FDF), CJK Symbols and Punctuation (U+3000–U+303F), Hiragana (U+3040–U+309F), Katakana (U+30A0–U+30FF), Bopomofo (U+3100–U+312F), Hangul Compatibility Jamo (U+3130–U+318F), Bopomofo Extended (U+31A0–U+31BF), CJK Strokes (U+31C0–U+31EF), Katakana Phonetic Extensions (U+31F0–U+31FF), CJK Unified Ideographs Extension A (U+3400–U+4DBF), CJK Unified Ideographs (U+4E00–U+9FFF), Hangul Jamo Extended-A (U+A960–U+A97F), Hangul Syllables (U+AC00–U+D7A3), Hangul Jamo Extended-B (U+D7B0–U+D7FF)"),
        ((0x1200, 0x137F), "Ethiopic", "Ge'ez script for Amharic, Tigrinya, and others"),
        ((0x1380, 0x139F), "Ethiopic Supplement", "Additional Ge'ez characters"),
        ((0x13A0, 0x13FF), "Cherokee", "Cherokee syllabary for the Cherokee language"),
        ((0x1400, 0x167F), "Unified Canadian Aboriginal Syllabics", "Syllabics for Indigenous Canadian languages"),
        ((0x1680, 0x169F), "Ogham", "Ogham script for Old Irish"),
        ((0x16A0, 0x16FF), "Runic", "Runic script for Germanic languages"),
        ((0x1700, 0x171F), "Tagalog", "Tagalog script (Baybayin) for Filipino languages"),
        ((0x1720, 0x173F), "Hanunoo", "Hanunoo script for Mangyan languages"),
        ((0x1740, 0x175F), "Buhid", "Buhid script for Mangyan languages"),
        ((0x1760, 0x177F), "Tagbanwa", "Tagbanwa script for Tagbanwa languages"),
        ((0x1780, 0x17FF), "Khmer", "Khmer script for the Khmer language"),
        ((0x1800, 0x18AF), "Mongolian", "Mongolian script for Mongolian and Manchu"),
        ((0x18B0, 0x18FF), "Unified Canadian Aboriginal Syllabics Extended", "Extended syllabics for Indigenous languages"),
        ((0x1900, 0x194F), "Limbu", "Limbu script for the Limbu language"),
        ((0x1950, 0x197F), "Tai Le", "Tai Le script for Tai Nüa language"),
        ((0x1980, 0x19DF), "New Tai Lue", "New Tai Lue script for the Tai Lue language"),
        ((0x19E0, 0x19FF), "Khmer Symbols", "Symbols used in Khmer script"),
        ((0x1A00, 0x1A1F), "Buginese", "Lontara script for Buginese and other languages"),
        ((0x1A20, 0x1AAF), "Tai Tham", "Tai Tham script for Northern Thai and Lao"),
        ((0x1AB0, 0x1AFF), "Combining Diacritical Marks Extended", "Extended diacritics for various scripts"),
        ((0x1B00, 0x1B7F), "Balinese", "Balinese script for the Balinese language"),
        ((0x1B80, 0x1BBF), "Sundanese", "Sundanese script for the Sundanese language"),
        ((0x1BC0, 0x1BFF), "Batak", "Batak script for Batak languages"),
        ((0x1C00, 0x1C4F), "Lepcha", "Lepcha script for the Lepcha language"),
        ((0x1C50, 0x1C7F), "Ol Chiki", "Ol Chiki script for the Santali language"),
        ((0x1C80, 0x1C8F), "Cyrillic Extended-C", "Extended Cyrillic for historical scripts"),
        ((0x1CC0, 0x1CCF), "Sundanese Supplement", "Additional Sundanese characters"),
        ((0x1CD0, 0x1CFF), "Vedic Extensions", "Characters for Vedic Sanskrit"),
        ((0x1D00, 0x1D7F), "Phonetic Extensions", "Additional phonetic symbols"),
        ((0x1D80, 0x1DBF), "Phonetic Extensions Supplement", "Supplementary phonetic symbols"),
        ((0x1DC0, 0x1DFF), "Combining Diacritical Marks Supplement", "Supplementary combining diacritics"),
        ((0x1E00, 0x1EFF), "Latin Extended Additional", "Additional Latin for Vietnamese and others"),
        ((0x1F00, 0x1FFF), "Greek Extended", "Extended Greek characters for polytonic Greek"),
        ((0x2C00, 0x2C5F), "Glagolitic", "Glagolitic script for Old Church Slavonic"),
        ((0x2C60, 0x2C7F), "Latin Extended-C", "Further Latin extensions"),
        ((0x2C80, 0x2CFF), "Coptic", "Coptic script for Coptic language"),
        ((0x2D00, 0x2D2F), "Georgian Supplement", "Additional Georgian characters"),
        ((0x2D30, 0x2D7F), "Tifinagh", "Tifinagh script for Berber languages"),
        ((0x2D80, 0x2DDF), "Ethiopic Extended", "Extended Ge'ez characters"),
        ((0x2DE0, 0x2DFF), "Cyrillic Extended-A", "Additional Cyrillic for early scripts"),
        ((0xA000, 0xA48F), "Yi Syllables", "Yi script syllables for the Yi language"),
        ((0xA490, 0xA4CF), "Yi Radicals", "Radicals for Yi script"),
        ((0xA4D0, 0xA4FF), "Lisu", "Lisu script for the Lisu language"),
        ((0xA500, 0xA63F), "Vai", "Vai syllabary for the Vai language"),
        ((0xA640, 0xA69F), "Cyrillic Extended-B", "Additional Cyrillic for minority languages"),
        ((0xA6A0, 0xA6FF), "Bamum", "Bamum script for the Bamum language"),
        ((0xA700, 0xA71F), "Modifier Tone Letters", "Tone letters for phonetic transcription"),
        ((0xA720, 0xA7FF), "Latin Extended-D", "Extended Latin for historical and minority languages"),
        ((0xA800, 0xA82F), "Syloti Nagri", "Syloti Nagri script for Sylheti language"),
        ((0xA840, 0xA87F), "Phags-pa", "Phags-pa script for Mongolian and Chinese"),
        ((0xA880, 0xA8DF), "Saurashtra", "Saurashtra script for the Saurashtra language"),
        ((0xA8E0, 0xA8FF), "Devanagari Extended", "Extended Devanagari for Vedic and other uses"),
        ((0xA900, 0xA92F), "Kayah Li", "Kayah Li script for Kayah languages"),
        ((0xA930, 0xA95F), "Rejang", "Rejang script for the Rejang language"),
        ((0xA980, 0xA9DF), "Javanese", "Javanese script for the Javanese language"),
        ((0xA9E0, 0xA9FF), "Myanmar Extended-B", "Extended Myanmar characters"),
        ((0xAA00, 0xAA5F), "Cham", "Cham script for Cham languages"),
        ((0xAA60, 0xAA7F), "Myanmar Extended-A", "Additional Myanmar characters"),
        ((0xAA80, 0xAADF), "Tai Viet", "Tai Viet script for Tai languages"),
        ((0xAAE0, 0xAAFF), "Meetei Mayek Extensions", "Extended Meetei Mayek for Manipuri"),
        ((0xAB00, 0xAB2F), "Ethiopic Extended-A", "Extended Ge'ez characters"),
        ((0xAB30, 0xAB6F), "Latin Extended-E", "Additional Latin for African languages"),
        ((0xAB70, 0xABBF), "Cherokee Supplement", "Additional Cherokee characters"),
        ((0xABC0, 0xABFF), "Meetei Mayek", "Meetei Mayek script for Manipuri"),
        ((0x10D00, 0x10D3F), "Hanifi Rohingya", "Hanifi Rohingya script for Rohingya language"),
        ((0x10E80, 0x10EBF), "Yezidi", "Yezidi script for the Yezidi language"),
        ((0x11000, 0x1107F), "Brahmi", "Brahmi script for ancient Indian languages"),
        ((0x11080, 0x110CF), "Kaithi", "Kaithi script for Hindi and related languages"),
        ((0x110D0, 0x110FF), "Sora Sompeng", "Sora Sompeng script for the Sora language"),
        ((0x11100, 0x1114F), "Chakma", "Chakma script for the Chakma language"),
        ((0x11150, 0x1117F), "Mahajani", "Mahajani script for historical accounting"),
        ((0x11180, 0x111DF), "Sharada", "Sharada script for Kashmiri and Sanskrit"),
        ((0x11200, 0x1124F), "Khojki", "Khojki script for Sindhi and Punjabi"),
        ((0x11280, 0x112AF), "Multani", "Multani script for Saraiki language"),
        ((0x112B0, 0x112FF), "Khudawadi", "Khudawadi script for Sindhi"),
        ((0x11300, 0x1137F), "Grantha", "Grantha script for Tamil and Sanskrit"),
        ((0x11400, 0x1147F), "Newa", "Newa script for the Newar language"),
        ((0x11480, 0x114DF), "Tirhuta", "Tirhuta script for Maithili language"),
        ((0x11580, 0x115FF), "Siddham", "Siddham script for Buddhist texts"),
        ((0x11600, 0x1165F), "Modi", "Modi script for Marathi"),
        ((0x11680, 0x116CF), "Takri", "Takri script for Dogri and other languages"),
        ((0x11700, 0x1174F), "Ahom", "Ahom script for the Ahom language"),
        ((0x11800, 0x1184F), "Dogra", "Dogra script for Dogri language"),
        ((0x11850, 0x1187F), "Dives Akuru", "Dives Akuru script for Maldivian languages"),
        ((0x11900, 0x1195F), "Nandinagari", "Nandinagari script for Sanskrit"),
        ((0x119A0, 0x119FF), "Zanabazar Square", "Zanabazar Square script for Mongolian"),
        ((0x11A00, 0x11A4F), "Soyombo", "Soyombo script for Mongolian and Tibetan"),
        ((0x11A50, 0x11AAF), "Pau Cin Hau", "Pau Cin Hau script for Tedim Chin"),
        ((0x11AB0, 0x11ABF), "Unified Canadian Aboriginal Syllabics Extended-A", "Extended syllabics for Indigenous languages"),
        ((0x11AC0, 0x11AFF), "Bhaiksuki", "Bhaiksuki script for Buddhist texts"),
        ((0x11C00, 0x11C6F), "Marchen", "Marchen script for Zhang-Zhung language"),
        ((0x11C70, 0x11CBF), "Masaram Gondi", "Masaram Gondi script for Gondi language"),
        ((0x11D00, 0x11D5F), "Gunjala Gondi", "Gunjala Gondi script for Gondi language"),
        ((0x11D60, 0x11DAF), "Makasar", "Makasar script for Makassarese language"),
        ((0x11EE0, 0x11EFF), "Lisu Supplement", "Additional Lisu characters"),
        ((0x16A40, 0x16A6F), "Mro", "Mro script for the Mro language"),
        ((0x16A70, 0x16ACF), "Tangsa", "Tangsa script for the Tangsa language"),
        ((0x16AD0, 0x16AFF), "Bassa Vah", "Bassa Vah script for the Bassa language"),
        ((0x16B00, 0x16B8F), "Pahawh Hmong", "Pahawh Hmong script for Hmong languages"),
        ((0x16E40, 0x16E9F), "Medefaidrin", "Medefaidrin script for the Ibibio language"),
        ((0x16F00, 0x16F9F), "Miao", "Miao script for Miao languages"),
        ((0x17000, 0x187FF), "Tangut", "Tangut script for the extinct Tangut language"),
        ((0x18800, 0x18AFF), "Tangut Components", "Components for Tangut characters"),
        ((0x18B00, 0x18CFF), "Khitan Small Script", "Khitan Small Script for Khitan language"),
        ((0x18D00, 0x18D7F), "Tangut Supplement", "Additional Tangut characters"),
        ((0x1B170, 0x1B2FF), "Nushu", "Nushu script for women’s writing in Chinese"),
        ((0x1E030, 0x1E06F), "Nyiakeng Puachue Hmong", "Nyiakeng Puachue Hmong script for Hmong"),
        ((0x1E100, 0x1E14F), "Toto", "Toto script for the Toto language"),
        ((0x1E2C0, 0x1E2FF), "Wancho", "Wancho script for the Wancho language"),
        ((0x1E4D0, 0x1E4FF), "Nag Mundari", "Nag Mundari script for Mundari language"),
        ((0x1E7E0, 0x1E7FF), "Ethiopic Extended-B", "Further extended Ge'ez characters"),
        ((0x1E800, 0x1E8DF), "Mende Kikakui", "Mende Kikakui script for Mende language"),
        ((0x1E900, 0x1E95F), "Adlam", "Adlam script for Fulani language")
    ]
    
    results = set()
    for char in text:
        # Get Unicode code point
        unicode_point = ord(char)
        unicode_hex = f"U+{unicode_point:04X}"
        
        # Find the script by checking Unicode block ranges
        script = "Unknown"
        for (start, end), script_name, des in unicode_blocks:
            if start <= unicode_point <= end:
                script = script_name
                break        
        if script != "Unknown":
            results.add(script)
    
    return list(results)

# ta
non_latin_languages = [
    Language.ARABIC,
    Language.ARMENIAN,
    Language.BELARUSIAN,
    Language.BENGALI,
    Language.BULGARIAN,
    Language.CHINESE,
    Language.GEORGIAN,
    Language.GREEK,
    Language.GUJARATI,
    Language.HEBREW,
    Language.HINDI,
    Language.JAPANESE,
    Language.KOREAN,
    Language.MARATHI,
    Language.PERSIAN,
    Language.PUNJABI,
    Language.RUSSIAN,   
    Language.TAMIL,
    Language.TELUGU,
    Language.THAI,
    Language.UKRAINIAN,
    Language.URDU,
    Language.ENGLISH,
    Language.FRENCH,
    Language.SPANISH,
    Language.PORTUGUESE,
    Language.ITALIAN,
    Language.GERMAN,
    Language.DUTCH,
    Language.VIETNAMESE
]

lingua_languages = non_latin_languages
detector = LanguageDetectorBuilder.from_languages(*lingua_languages).with_preloaded_language_models().with_minimum_relative_distance(0.99).build()


def clean_body_text(text):
    if not text:
        return ""

    lines = text.splitlines()
    cleaned_lines = []

    for line in lines:
        if re.search(r'https?://\S+|www\.\S+', line):
            continue
        if re.search(r'!\[.*?\]\(.*?\)', line):
            continue

        line = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', line)
        line = re.sub(r'`{1,3}[^`]+`{1,3}', '', line)
        line = re.sub(r'```[\s\S]+?```', '', line)
        line = re.sub(r'[^\p{L}\p{N}\s.,!?\'\"-]', '', line)

        cleaned_lines.append(line.strip())

    return ' '.join(cleaned_lines).strip()

def process_hour(hour, year, month, day, download_dir, ids):
    local_counts = defaultdict(int)
    scripts_length = defaultdict(int)
    could_not_process = []
    messages = []  # New list to store (language, message)
    day_str = f"{year}-{month:02d}-{day:02d}"
    filename = f"{day_str}-{hour}.json.gz"
    filepath = os.path.join(download_dir, filename)

    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    event = json.loads(line)
                    event_type = event.get("type")
                    if event_type in {"IssuesEvent", "IssueCommentEvent","PullRequestEvent", "PullRequestReviewEvent", "PullRequestReviewCommentEvent"}:
                        if 'bot' not in event.get("actor", {}).get("login", ""):
                            shoudl_process = True
                            payload = event.get("payload", {})
                            title, body, item_id = "", "", None
                            if event_type == "IssuesEvent":
                                issue = payload.get("issue", {})
                                title = issue.get("title", "")
                                body = issue.get("body", "")
                                item_id = issue.get("id", None)
                                if 'bot' in issue.get("user", {}).get("login", ""):
                                    shoudl_process = False
                            elif event_type == "IssueCommentEvent":
                                issue = payload.get("comment", {})
                                title = issue.get("title", "")
                                body = issue.get("body", "")
                                item_id = issue.get("id", None)
                                if 'bot' in issue.get("user", {}).get("login", ""):
                                    shoudl_process = False
                            elif event_type == "PullRequestReviewCommentEvent":
                                pr = payload.get("comment", {})
                                title = pr.get("title", "")
                                body = pr.get("body", "")
                                item_id = pr.get("id", None)
                                if 'bot' in pr.get("user", {}).get("login", ""):
                                    shoudl_process = False
                            elif event_type == "PullRequestReviewEvent":
                                pr = payload.get("review", {})
                                title = pr.get("title", "")
                                body = pr.get("body", "")
                                item_id = pr.get("id", None)
                                if 'bot' in pr.get("user", {}).get("login", ""):
                                    shoudl_process = False
                            elif event_type == "PullRequestEvent":
                                pr = payload.get("pull_request", {})
                                title = pr.get("title", "")
                                body = pr.get("body", "")
                                item_id = pr.get("id", None)
                                if 'bot' in pr.get("user", {}).get("login", ""):
                                    shoudl_process = False
                            if shoudl_process:
                                cleaned_body = clean_body_text(body)
                                message = f"{title}\n{cleaned_body}".strip()
                                if item_id and item_id in ids:
                                    scripts = get_unicode_and_script(message)
                                    if scripts:
                                        scripts_length[len(scripts)] += 1
                                        if len(scripts) == 1:
                                            detection = detector.compute_language_confidence_values(message)
                                            has_detected = False
                                            if detection:
                                                detected_lang = detection[0]
                                                if detected_lang.value >= 0.9:
                                                    lang_name = detected_lang.language.name.lower()
                                                    local_counts[lang_name] += 1
                                                    messages.append((lang_name, message))
                                                    has_detected = True
                                            if not has_detected:
                                                could_not_process.append(item_id)
                                        elif len(scripts) >= 2:
                                            detection = detector.detect_multiple_languages_of(message)
                                            languages = set()
                                            has_detected = False
                                            if detection:
                                                for result in detection:
                                                    confidence_value = detector.compute_language_confidence(
                                                        message[result.start_index:result.end_index],
                                                        result.language
                                                    )
                                                    if confidence_value > 0.9:
                                                        lang = result.language.name.lower()
                                                        if lang not in languages:
                                                            local_counts[lang] += 1
                                                            messages.append((lang, message))
                                                            languages.add(lang)
                                                            has_detected = True
                                            if not has_detected:
                                                could_not_process.append(item_id)
                except Exception:
                    pass
    except Exception:
        return None
    return local_counts, could_not_process, messages


if __name__ == "__main__":
    ids_could_not_process = []
    for day in range(1, 32):
        try:
            local_counts = defaultdict(int)
            all_detected_messages = []  # Store (language, message)
            start_time = time.time()
            download_dir = f"data/gharchive_{year}_{month:02d}"
            day_str = f"{year}_{month:02d}_{day:02d}"
            df = pd.read_csv(f"id_files/combined_non_english_ids_{day_str}.csv")
            ids = df['id'].tolist()
            eng_df = pd.read_csv(f"id_files/combined_english_ids_{day_str}.csv")
            eng_ids = eng_df['id'].tolist()
            ids.extend(eng_ids)
            print(f"Processing day {day_str} with {len(ids)} IDs...")
            func = partial(process_hour, year=year, month=month, day=day, download_dir=download_dir, ids=ids)

            with Pool(processes=min(cpu_count(), 24)) as pool:
                for result in tqdm(pool.imap_unordered(func, range(24)), total=24, desc="Processing hours"):
                    if result:
                        local, could_not_process, messages = result
                        for k, v in local.items():
                            local_counts[k] += v
                        ids_could_not_process.extend(could_not_process)
                        all_detected_messages.extend(messages)

            print(f"Day {day_str} processed in {time.time() - start_time:.2f} seconds. Counts: {dict(local_counts)}")
            print("Could not process IDs:", len(ids_could_not_process))
            df_counts = pd.DataFrame.from_dict(local_counts, orient='index', columns=['count']).reset_index()
            df_counts.columns = ['script', 'count']
            df_counts.to_csv(f"resul_files/script_count_{year}_{month:02d}_{day:02d}.csv", index=False)
            print(f"Counts saved to script_count_{year}_{month:02d}_{day:02d}.csv")

            df_messages = pd.DataFrame(all_detected_messages, columns=["language", "message"])
            df_messages.to_csv(f"resul_files/messages_with_languages_{year}_{month:02d}_{day:02d}.csv", index=False)
            print(f"Messages saved to messages_with_languages_{year}_{month:02d}_{day:02d}.csv")

        except Exception as e:
            print(f"Error processing day {day_str}: {e}")
            continue

    # print("Could not process IDs:", len(ids_could_not_process))
    with open(f"resul_files/could_not_process_ids_{year}_{month:02d}.txt", "w") as f:
        for item_id in ids_could_not_process:
            f.write(f"{item_id}\n")



    