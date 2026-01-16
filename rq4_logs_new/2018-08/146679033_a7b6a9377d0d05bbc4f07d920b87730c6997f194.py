import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt

import mojimoji as mj
from natto import MeCab
from sklearn.feature_extraction.text import CountVectorizer

import pyspark
from pyspark import SQLContext, sql
from pyspark.sql.types import *
from pyspark.ml.linalg import Vectors

import sys

tagger = MeCab('-d /usr/local/lib/mecab/dic/mecab-ipadic-neologd')

ng_noun = ["これ", "よう",  "こと", "の", "もの", "それ", "とき"] # お好みで

appName = 'association'
conf = pyspark.SparkConf().setAppName(appName).setMaster('local[4]').set("spark.executor.cores", "2")
sc = pyspark.SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)


def _sentence2bow(sentence):
  """
  文を形態素解析してBagOfWordsに変換
  @param sentence: text
    自然言語の文
  @return bag: list
    語形変化が修正された単語のリスト
  """
  bag = []
  # e.g. 動詞:surface="滑れ", feature="動詞,自立,*,*,一段,未然形,滑れる,スベレ,スベレ"
  for node in tagger.parse(sentence, as_nodes=True):
    features = node.feature.split(",")
    if features[0] == "名詞":
      noun = mj.zen_to_han(node.surface).encode('utf-8')
      if noun not in ng_noun:
        bag.append(node.surface)

  # 文書中の重複はまとめてしまう
  return list(set(bag))


file = sys.argv[1]
df = pd.read_csv(file, delimiter='\t', names=["Text"],
  dtype = {'Text':'object'})

documents = []
for i, row in df.iterrows():
  documents.append(' '.join(_sentence2bow(row['Text'])))

vectorizer = CountVectorizer(max_df=0.5, min_df=2, max_features=100)
X = vectorizer.fit_transform(documents)
features = vectorizer.get_feature_names()

colnames = ['doc' + str(i) for i in range(0, X.shape[0])]

index = 'word'
pdf = pd.DataFrame(X.T.toarray())
pdf[index] = features

def _createDataFrame(df, colnames, index):
  idx = str(index)
  col = [col + '_' + idx for col in colnames]
  fields = [StructField(field_name, IntegerType(), True) for field_name in col]
  fields.append(StructField("word" + "_" + idx , StringType(), True))
  sdf = sqlContext.createDataFrame(pdf, StructType(fields))
  return sdf

sdf1 = _createDataFrame(pdf, colnames, 1)
sdf2 = _createDataFrame(pdf, colnames, 2)

joined = sdf1.join(sdf2, sdf1.word_1 < sdf2.word_2)

result = joined.rdd.map(lambda x: (
    x["word_1"],
    x["word_2"],
    float(sum([min(x[c +'_1'], x[c + '_2']) for c in colnames])) /
    float(sum([max(x[c +'_1'], x[c + '_2']) for c in colnames]))
    )).filter(lambda x: x[2] > 0.01).collect()

# build network 
G = nx.Graph()
G.add_nodes_from(features, size=10)

# edgeの追加
edge_threshold = 0.15
for i, j, w in result:
    if w > edge_threshold:
        G.add_edge(i, j, weight=w)

# 孤立したnodeを削除
#isolated = [n for n in G.nodes if len([ i for i in nx.all_neighbors(G, n)]) == 0]
#for n in isolated:
#    G.remove_node(n)

plt.figure(figsize=(20,20))
pos = nx.spring_layout(G, k=0.3) # k = node間反発係数

# nodeの大きさ
node_size = [d["size"]*50 for (n,d) in G.nodes(data=True)]
nx.draw_networkx_nodes(G, pos, node_color="b",alpha=0.3, node_size=node_size)

# 日本語ラベル
nx.draw_networkx_labels(G, pos, fontsize=14, font_family="IPAGothic", font_weight="bold")

# エッジの太さ調節
edge_width = [ d["weight"]*20 for (u,v,d) in G.edges(data=True)]
nx.draw_networkx_edges(G, pos, alpha=0.4, edge_color="c", width=edge_width)

plt.axis('off')
plt.show()