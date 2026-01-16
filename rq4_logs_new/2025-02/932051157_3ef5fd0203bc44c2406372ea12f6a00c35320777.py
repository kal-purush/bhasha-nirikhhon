import os
import openai
import json
import time
import random

from sequence_aligner.labelset import LabelSet
from sequence_aligner.dataset import TrainingDatasetCRF
from sequence_aligner.containers import TraingingBatch
from transformers import BertTokenizerFast
#%%
from seqeval.metrics import f1_score
from seqeval.metrics import precision_score
from seqeval.metrics import accuracy_score
from seqeval.metrics import recall_score
from seqeval.metrics import classification_report
# from util.process import ids_to_labels,Metrics,Metrics_e
from seqeval.scheme import BILOU

def subset(alist, idxs):
    sub_list = []
    for idx in idxs:
        sub_list.append(alist[idx])
    return sub_list

    # Correcting start and end tags    对 result_list 中的每个元素进行处理，修正起始和结束标签。
    #代码确保了注释在文本中的位置不重叠，并且更新了注释的起始位置和结束位置
def re_position(result_list):
    for i, item in enumerate(result_list):
        text = item['content']
        annos = item['annotations']  #{}{}
        sorted_annos = sorted(annos, key=lambda x: x['start']) #按start值对每组排序
        value_list = []
        start_list = []
        last_start = -1
        drop_list = []
        for indx, sub_annos in enumerate(sorted_annos): #对于每组{"start": 74, "end": 84, "value": "Reddit Inc", "tag": "Neutral"}

            value = sub_annos['value']   #"value": "Reddit Inc"
            # if value not in value_list:
            #     start = text.find(value)
            # else:
            #     index_list = []
            #     for j, v in enumerate(value_list):
            #         if v == value:
            #             index_list.append(j)
            #     sub_start = subset(start_list, index_list)
            #     last_start = max(sub_start)  #使用 max(sub_start) 来找到子列表中的最大值，即之前相同值的最后一个起始位置，然后将其加1作为新的起始位置。
            #     start = text.find(value, last_start + 1)  #保证新的注释不会与之前的注释重叠
            start = text.find(value, last_start+1)
            sub_annos['start'] = start
            sub_annos['end'] = start + len(value)
            last_start = start + len(value)

            value_list.append(value)
            start_list.append(start)
    return result_list


def get_compare(data_path):
    raw = json.load(open(data_path, encoding="utf-8"))
    compare_list = []  #存储真实标签
    for item in raw:
        comp = {}
        comp["content"] = item["content"]
        comp_anno_list = []
        comp_strs = item["output"].split('\n')
        for res in comp_strs:  # 对于每一个实体组 {"start": 74, "end": 84, "value": "Reddit Inc", "tag": "Neutral"}
            index_left = res.find('{')
            index_right = res.find('}')
            if index_right == -1 or index_left == -1:
                continue
            res = res[index_left:index_right + 1]
            sub_json = json.loads(res)
            comp_anno_list.append(sub_json)
        comp["annotations"] = comp_anno_list
        compare_list.append(comp)
        compare_list = re_position(compare_list)  #有的句子中含有非法字符，需要重新确定一下位置，以与预测标签可以对应。

    # pre_raw = json.load(open(data_path+'save_pres_FinEntity_2024-04-03-21-02-48.json'))
    pre_raw = json.load(open(data_path, encoding="utf-8"))
    result_list= []  #存储预测标签
    for it in pre_raw:
        result = {}
        result["content"] = it["content"]
        anno_list = []
        pre_strs = it["pre"].split('\n')
        # pre_strs = it["output"].split('\n')
        for res in pre_strs:  #对于每一个实体组 {"start": 74, "end": 84, "value": "Reddit Inc", "tag": "Neutral"}
            index_left = res.find('{')
            index_right = res.find('}')
            if index_right == -1 or index_left == -1:
                continue
            res = res[index_left:index_right + 1]
            sub_json = json.loads(res)
            anno_list.append(sub_json)
        result["annotations"] = anno_list

        result_list.append(result)

    print("LLM预测结果中annotations：", result_list[11]["annotations"])
    result_list = re_position(result_list)
    print("修正起始位置后中annotations：", result_list[11]["annotations"])
    print("测试集中真实的 annotations：", compare_list[11]["annotations"])

    print(len(compare_list))
    print(len(result_list))
    return compare_list, result_list

def check_anno(compare_list, result_list):
    # %%找到 result_list 中所有具有负起始位置的注释，然后打印包含这些注释的整个字典项，以便进行进一步的检查或处理
    for idx, item in enumerate(result_list):
        annos = item['annotations']
        for a in annos:
            if a['start'] < 0:
                print("result_list:", item)

    # %%只保留起始位置大于等于 0 的组
    for idx, item in enumerate(result_list):
        annos = item['annotations']
        drop_list = []
        item['annotations'] = [x for x in annos if x['start'] >= 0]

    for example in result_list:
        for annotation in example['annotations']:
            # We expect the key of label to be label but the data has tag
            annotation['label'] = annotation['tag']

    # %%找到 result_list 中所有具有负起始位置的注释，然后打印包含这些注释的整个字典项，以便进行进一步的检查或处理
    for idx, item in enumerate(compare_list):
        annos = item['annotations']
        for a in annos:
            if a['start'] < 0:
                print("compare_list:", item)

    # %%只保留起始位置大于等于 0 的组 , 原数据集中有一条数据的二元组有重复导致reposition后x['start']=-1，导致报错，compare_list经过此函数就可以不报错。
    for idx, item in enumerate(compare_list):
        annos = item['annotations']
        drop_list = []
        item['annotations'] = [x for x in annos if x['start'] >= 0]

    for example in compare_list:
        for annotation in example['annotations']:
            # We expect the key of label to be label but the data has tag
            annotation['label'] = annotation['tag']

    return compare_list, result_list

def error_analyze(data_path, compare_list, result_list):
    print("\n\n\n#################################错误样本分析#####################################\n\n\n")
    # print(compare_list[0])
    # print(result_list[0])
    # 打开一个txt文件用于写入
    error_index_list = []
    error_path = data_path.replace("save_pres", "error_analyze_save_pres").replace(".json", ".txt")
    with open(error_path, 'w', encoding='UTF-8') as file:
        print(compare_list[0])

        for index, (item1, item2) in enumerate(zip(compare_list, result_list)):

            item1['annotations'] = sorted(item1['annotations'], key=lambda x: x['start'])

            if item1['annotations'] != item2['annotations']:
                error_index_list.append(index)
                # print(str(item1['content']))
                file.write(str(item1['content']) + "\n")
                # print("真实标签:", item1['annotations'])
                file.write("真实标签:" + str(item1['annotations']) + "\n")
                # print("预测标签:", item2['annotations'])
                file.write("预测标签:" + str(item2['annotations']) + "\n")
                # print()
                file.write("\n")

    print("错误样本已写入", error_path, "文件中")
    return error_index_list

def contral_error_rate(data_path,error_index_list):
    raw_data = json.load(open(data_path, encoding="utf-8"))
    compare_list = []  # 存储真实标签
    rate = 0.9
    for index2 ,item in enumerate(raw_data):
        if index2 in error_index_list:
            compare_list.append(item)
        else:
            if random.random() < rate:
                compare_list.append(item)
    # 将 compare_list 写入 JSON 文件
    train_pre_save_path = data_path.replace("save_pres", "train_pre_"+str(rate))
    with open(train_pre_save_path, 'w', encoding='utf-8') as json_file:
        json.dump(compare_list, json_file, ensure_ascii=False, indent=4)

    print("去除部分正确样本后的训练集预测数据，已成功写入", train_pre_save_path, "文件中")





def report(data_path, compare_list, result_list):
    # 计算得分
    # tokenizer = BertTokenizerFast.from_pretrained('yiyanghkust/finbert-pretrain')
    tokenizer = BertTokenizerFast.from_pretrained('finbert')
    if('FinEntity' in data_path):
        label_set = LabelSet(labels=["Neutral", "Positive", "Negative"])  # label in FinEntity
    elif('SEntFiN' in data_path):
        label_set = LabelSet(labels=["neutral", "positive", "negative"])  # label in SEntFiN
    elif ('EFSA' in data_path):
        label_set = LabelSet(labels=["中立", "正面", "负面"])# label in EFSA

    dataset = TrainingDatasetCRF(data=compare_list, tokenizer=tokenizer, label_set=label_set, tokens_per_batch=128)
    dataset_openai = TrainingDatasetCRF(data=result_list, tokenizer=tokenizer, label_set=label_set,
                                        tokens_per_batch=128)

    label_list = []
    pred_label_list = []
    for i in range(len(dataset)):
        sub_list = []
        pred_sub_list = []

        for m in dataset[i].labels:
            if m == -1:
                continue
            else:
                sub_list.append(label_set.ids_to_label[m])

        if i == 0:
            print(dataset[i])
            print(dataset[i].labels)
            print(sub_list)

        for n in dataset_openai[i].labels:
            if n == -1:
                continue
            else:
                if n == None:
                    n = 0
                pred_sub_list.append(label_set.ids_to_label[n])
        label_list.append(sub_list)
        pred_label_list.append(pred_sub_list)

    report = classification_report(label_list, pred_label_list, mode='strict', scheme=BILOU)
    print(report)



# data_path = "../IstructABSA_result/finentity/save_pres_FinEntity_result_eval.json"
# data_path = "../IstructABSA_result/sentfin/save_pres_SEntFiN_result_eval.json"
# data_path = "../train_pre/SEntFiN/save_pres_SEntFiN_2024-07-08-17-30-08.json"
data_path = "../train_pre/EFSA/save_pres_EFSA_2024-07-08-18-58-04.json"
# data_path = "../train_pre/FinEntity/save_pres_FinEntity_2024-07-10-10-36-28.json"

# data_path = "../correct_result/SEntFiN/save_pres_SEntFiN_correct_2024-07-10-03-03-26.json"
# data_path = "../correct_result/FinEntity/save_pres_FinEntity_correct_2024-07-10-12-00-17.json"

compare_list, result_list = get_compare(data_path)  # 获取真实标签和预测标签并修正位置
compare_list, result_list = check_anno(compare_list, result_list) #检查不合规的位置信息，只保留起始位置大于等于 0 的组

error_index_list = error_analyze(data_path, compare_list, result_list)  #错误分析,将错误的样本写入txt文件
contral_error_rate(data_path, error_index_list)# 随机删除部分正确样本，使训练集错误率和测试集相当
report(data_path, compare_list, result_list)  #报告序列标注得分
