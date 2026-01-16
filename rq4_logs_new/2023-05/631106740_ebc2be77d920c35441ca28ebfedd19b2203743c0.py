import os
import re

import PyPDF2
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords, wordnet
from collections import Counter

# 文件夹路径
folder_path = 'D:/workspace/python/wordparse/book'

# 停用词列表
stop_words = set(stopwords.words("english"))

# 单词计数器
word_counter = Counter()

# 遍历文件夹中的所有PDF文件
for file_name in os.listdir(folder_path):
    if file_name.endswith(".pdf"):
        file_path = os.path.join(folder_path, file_name)

        # 打开PDF文件
        with open(file_path, "rb") as file:
            pdf_reader = PyPDF2.PdfReader(file)

            # 遍历PDF中的每一页
            for page in pdf_reader.pages:
                # 提取文本内容
                text = page.extract_text()

                # 分词并去除停用词
                #tokens = word_tokenize(text)
                #tokens = [token.lower() for token in tokens if token.isalpha() and token.lower() not in stop_words]
                word_list = re.findall(r'\b\w+\b', text)
                words = [word.lower() for word in word_list if word.isalpha() and word.lower() not in stop_words
                         and len(word) > 1 and not word.isdigit()]

                tokens = [word for word in words if wordnet.synsets(word)]

                # 更新单词计数器
                word_counter.update(tokens)

# 将单词按出现次数排序
sorted_words = sorted(word_counter.items(), key=lambda x: x[1], reverse=True)

# 将排序结果写入txt文件
output_file = "D:/workspace/python/wordparse/book/output.txt"
with open(output_file, "w", encoding="utf-8") as file:
    for word, count in sorted_words:
        #tokens_with_meaning = []
        #synsets = wordnet.synsets(word)
        #if synsets:
            #meaning = [synset.lemma_names('cmn') for synset in synsets]
            #file.write(f"{word}: {meaning} : {count}\n")

        file.write(f"{word}: {count}\n")

print("Output written to", output_file)