#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Author : lianggq
# @Time  : 2019/7/9 11:34
# @FileName: mode_test.py
import pandas as pd
import jieba
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import make_pipeline
from sklearn import metrics

# 读取文件数据
df = pd.read_csv('./data/data.csv', encoding='gb18030')


def make_lable(df):
    # 根据评分进行分类标签
    df['lable'] = df['grade'].apply(lambda x: 1 if x > 3 else 0)


def chinese_cut(text):
    """
    进行中文分词
    :param text: 中文数据
    :return: 分词后的数据
    """
    return ' '.join(jieba.cut(text))


# 调用函数进行标签设置
make_lable(df)
X = df[['comment']]
y = df.lable
# 对每行数据进行分词
X['cut_comment'] = X.comment.apply(chinese_cut)
# 把数据集按随机比例进行拆开
x_train, x_test, y_train, y_test = train_test_split(X, y, random_state=2)


def get_stopwords(filename):
    """
    从中文停用词表里面，把停用词作为列表格式保存并返回
    :param filename:中文停用词路径及文件名
    :return:停用词列表
    """
    with open(filename, 'r', encoding='utf-8') as r:
        stopwords = r.read()
        stopwords_list = stopwords.split('\n')
        cut_stopword_list = [i for i in stopwords_list]
        return cut_stopword_list


stopwords = get_stopwords('./StopWords/哈工大停用词表.txt')
max_df = 0.8  # 在超过这一定比例的文档中出现的关键词（过于平凡），去除掉。
min_df = 2  # 在低于一定数量的文档中出现的词，去掉

# CountVectorizer向量化工具，依据词语出现频率转化向量,并且添加停用词去除功能
vect = CountVectorizer(
    max_df=max_df,
    min_df=min_df,
    token_pattern=u'(?u)\\b[^\\d\\W]\\w+\\b',
    stop_words=frozenset(stopwords))
# 词向量表
word_matrix = pd.DataFrame(vect.fit_transform(x_train.cut_comment).toarray(), columns=vect.get_feature_names())

# 这里采用贝叶斯进行分类
nb = MultinomialNB()
pipe = make_pipeline(vect, nb)
# 交叉验证验证准确率
accuracy = cross_val_score(pipe, x_train.cut_comment, y_train, cv=5, scoring='accuracy').mean()

# 模型的创建
pipe.fit(x_train.cut_comment, y_train)
# 测试数据的预测
prediction = pipe.predict(x_test.cut_comment)

# 下面两步都是对模型识别度的判断
# 对预测结果的准确率进行识别
prediction_prob = metrics.accuracy_score(y_test, prediction)
# 混淆矩阵
prediction_matrix = metrics.confusion_matrix(y_test, prediction)

if __name__ == '__main__':
    print(prediction_prob)
    print(prediction_matrix)