import jieba
import time

start = time.perf_counter()

def jaccard_similarity(text1, text2):
    # 使用jieba分词将文本转换为词语集合（集合可以确保唯一性）
    words1 = set(jieba.cut(text1))
    words2 = set(jieba.cut(text2))

    # 计算Jaccard相似度
    intersection = len(words1.intersection(words2))
    union = len(words1) + len(words2) - intersection
    similarity = intersection / union

    return similarity

'''with open("orig.txt", "r", encoding='utf-8') as f:
    original_text = f.read()

with open("orig_0.8_add.txt", "r", encoding='utf-8') as f:
    plagiarized_text = f.read()'''

original_text = "今天是星期天，天气晴，今天晚上我要去看电影。"
plagiarized_text = "今天是星期天，天气晴，今天晚上我要去看电影。"
similarity = jaccard_similarity(original_text, plagiarized_text)
print(f"Jaccard相似度: {similarity:.2f}")

end = time.perf_counter()
print(f"程序运行时间:{end - start:0.4f}秒")