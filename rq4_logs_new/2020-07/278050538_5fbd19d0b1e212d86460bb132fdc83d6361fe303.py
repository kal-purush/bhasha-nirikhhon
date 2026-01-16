# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %%
import json
import os
import re
from CorpusStat import CorpusStat
from SumCollector import SumCollector

summary_pattern = r'summary{{(.*)}}'
text_pattern = r'text{{(.*)}}'

data_path = '/search/odin/data/summarization_data/'

dealed_news_2016_train = data_path + 'news2014-2016zh/dealed_news_2016/dealed_corpus_news2016_train'
dealed_news_2016_test = data_path + 'news2014-2016zh/dealed_news_2016/dealed_corpus_news2016_test'
dealed_news_2016_valid = data_path + 'news2014-2016zh/dealed_news_2016/dealed_corpus_news2016_valid'

news2016zh_train = data_path +'zh_data/news2016zh_train.json'

chinese_abstractive_corpus_path = data_path + 'chinese_abstractive_corpus/copus/'

nlpcc_2018_train = data_path + 'TTNewsCorpus_NLPCC2018/toutiao4nlpcc/train_with_summ.txt'

collector1k = SumCollector(1000)
collector5k = SumCollector(5000)
collector10k = SumCollector(10000)

collectors = [collector1k, collector5k, collector10k]

stat = CorpusStat()


# %%
print("fetching dataset: dealed_news_2016_train")
stat.clear()
with open(dealed_news_2016_train, 'r', encoding='utf-8') as file:
    lines = file.readlines()
    for line in lines:
        if(stat.text_stat[0] == 0):
            print(line)
        jsonline = json.loads(line)
        # print(jsonline)
        stat.stat_once(jsonline["abstract"], jsonline["document"])
        for collector in collectors:
            collector.add_summ_pair(jsonline["abstract"], jsonline["document"])
    stat.print_all()


# %%
print("fetching dataset: news2016zh_train")
stat.clear()
with open(news2016zh_train, 'r', encoding='utf-8') as file:
    lines = file.readlines()
    for line in lines:
        if(stat.text_stat[0] == 0):
            print(line)
        jsonline = json.loads(line.strip())
        # print(jsonline)
        stat.stat_once(jsonline["title"], jsonline["content"])
        for collector in collectors:
            collector.add_summ_pair(jsonline["title"], jsonline["content"])
    stat.print_all()


# %%
print("fetching dataset: Chinese Abstractive Corpus")
file_list = os.listdir(chinese_abstractive_corpus_path)
stat.clear()
for filename in file_list:
    with open(chinese_abstractive_corpus_path + filename, 'r', encoding="utf-8") as file:
        file_lines = file.readlines()
        if(stat.text_stat[0] == 0):
            print(file_lines)
        raw_summary = file_lines[0]
        raw_text = file_lines[1]

        summary = re.match(summary_pattern, raw_summary).group(1)
        text = re.match(text_pattern, raw_text).group(1)
        stat.stat_once(summary, text)
        for collector in collectors:
            collector.add_summ_pair(summary, text)

stat.print_all()


# %%
print("fetching dataset: NLPCC 2018 train")
stat.clear()
with open(nlpcc_2018_train) as file:
    line = file.readline().strip('\r\n')
    while line:
        jsonline = json.loads(line)
        if(stat.text_stat[0] == 0):
            print(jsonline)
        stat.stat_once(jsonline["summarization"], jsonline["article"])
        for collector in collectors:
            collector.add_summ_pair(jsonline["summarization"], jsonline["article"])
        line = file.readline().strip('\r\n')
stat.print_all()


# %%
for collector in collectors:
    collector.print_stats()


# %%
collector1k.dump_data(data_path + "custom_data/integrated_train_data_1k")
collector5k.dump_data(data_path + "custom_data/integrated_train_data_5k")
collector10k.dump_data(data_path + "custom_data/integrated_train_data_10k")


# %%

