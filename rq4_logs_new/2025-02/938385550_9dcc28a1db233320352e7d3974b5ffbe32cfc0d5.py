import unicodedata
import string
import time
from functools import lru_cache
import concurrent.futures
import numpy as np
import csv

# اصلاح جدول ترجمه که نیم‌فاصله رو نگه داره
COMBINED_TRANS = str.maketrans('', '', string.punctuation + string.whitespace.replace('\u200c', '') + '@#')
NUMBER_TRANS = str.maketrans('0123456789', '۰۱۲۳۴۵۶۷۸۹')

class TurboNormalizer:
    def __init__(self):
        self.combined_trans = COMBINED_TRANS
        self.number_trans = NUMBER_TRANS
        self.diacritics = set('\u064b\u064c\u064d\u064e\u064f\u0650\u0651\u0652')
        self.suffixes = {'ها', 'ای', 'تر', 'تری', 'ترین'}
    
    @lru_cache(maxsize=16384)
    def normalize_word(self, word: str) -> str:
        word = unicodedata.normalize('NFKC', word)
        word = ''.join(c for c in word if c not in self.diacritics)
        return word.translate(self.combined_trans).translate(self.number_trans).lower()
    
    def _normalize_chunk(self, sentence: str) -> str:
        sentence = unicodedata.normalize('NFKC', sentence.lower())
        sentence = ''.join(c for c in word if c not in self.diacritics)
        sentence = sentence.translate(self.combined_trans).translate(self.number_trans)
        
        # کاهش تکرار حروف
        result = []
        last_char = ''
        count = 0
        for char in sentence:
            if char == last_char:
                count += 1
                if count <= 2:
                    result.append(char)
            else:
                result.append(char)
                last_char = char
                count = 1
        sentence = ''.join(result)
        
        # جدا کردن کلمات با حفظ نیم‌فاصله
        words = sentence.split()
        output = []
        i = 0
        while i < len(words):
            word = words[i]
            # مدیریت "می" و "نمی"
            if word.startswith('می') or word.startswith('نمی'):
                prefix = 'می' if word.startswith('می') else 'نمی'
                rest = word[len(prefix):]
                if rest:
                    word = prefix + '\u200c' + rest
            
            # نیم‌فاصله‌گذاری پسوندها
            if i + 1 < len(words) and words[i + 1] in self.suffixes:
                output.append(word + '\u200c' + words[i + 1])
                i += 2
            else:
                output.append(word)
                i += 1
        
        return ' '.join(output)
    
    def normalize_sentence(self, sentence: str) -> str:
        return self._normalize_chunk(sentence)
    
    def normalize_bulk(self, texts: list) -> list:
        if len(texts) < 100:
            return [self._normalize_chunk(t) for t in texts]
        text_array = np.array(texts, dtype=object)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = list(executor.map(self._normalize_chunk, text_array))
        return results

def precision_timer(func):
    def wrapper(*args, **kwargs):
        func(*args, **kwargs)
        start = time.perf_counter_ns()
        result = func(*args, **kwargs)
        elapsed = (time.perf_counter_ns() - start) / 1e9
        return result, elapsed
    return wrapper

# دیتاست نمونه توییتر
twitter_dataset = [
    "سلام چطوری؟ امروز خیلی هوا خوبه!",
    "این فیلم جدید رو دیدم واقعا عالی بود!!!",
    "چرا انقدر ترافیک زیاده؟؟؟ خسته شدم",
    "کاش یه روز بدون استرس داشته باشیم...",
    "دیروز یه کتاب خوندم خیلی جالب بود #کتاب",
    # ... بقیه دیتاست همونیه که قبلاً دادم، برای کوتاه بودن فقط ۵ تا نمونه گذاشتم
    # می‌تونی دیتاست کامل رو از پیام قبلی کپی کنی
]

if __name__ == "__main__":
    normalizer = TurboNormalizer()
    
    # دیتاست کامل رو اینجا کپی کن یا همون ۵ تا رو تست کن
    twitter_dataset = twitter_dataset  # جایگزین با دیتاست کامل اگه خواستی
    
    tests = {
        "تک کلمه ساده": ("TEST!", normalizer.normalize_word),
        "جمله استاندارد": ("این یک متن TEST با علائم !@# سجاوندی و   فاصله‌های اضافه است!!!", normalizer.normalize_sentence),
        "پردازش گروهی": (twitter_dataset, normalizer.normalize_bulk),
        "توییت نمونه": ("سلام @ali چطوری؟ #جالب اینو ببینممم https://t.co/test 😂 ۳.۱۴", normalizer.normalize_sentence)
    }
    
    with open('normalized_tweets.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Original Text', 'Normalized Text', 'Execution Time (s)'])
        
        for name, (data, func) in tests.items():
            result, elapsed = precision_timer(func)(data)
            print(f"✅ تست {name}")
            print(f"⏱ زمان اجرا: {elapsed:.10f} ثانیه")
            print(f"📊 حجم داده: {len(data) if isinstance(data, list) else len(data.split())}")
            
            if isinstance(result, str):
                writer.writerow([data, result, elapsed])
                print(f"🎯 نمونه خروجی: {result[:75] + '...' if len(result) > 75 else result}")
            else:
                for orig, norm in zip(data, result):
                    writer.writerow([orig, norm, elapsed / len(data)])
                print(f"🎯 نمونه خروجی: {result[0][:75] + '...' if len(result[0]) > 75 else result[0]}")
            print("-" * 80)
    
    print("نتایج در فایل 'normalized_tweets.csv' ذخیره شد!")