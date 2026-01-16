#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
可视化垃圾文本检测模型的验证结果
"""

import os
import csv
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# 结果保存文件
RESULTS_FILE = 'validation_results2.csv'
# 图表保存路径
PLOT_FILE = 'validation_plot2.png'

def read_results_from_csv(csv_file):
    """
    从 CSV 文件中读取验证结果
    """
    results = []
    with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            results.append(row)
    return results

def visualize_results(results):
    """
    可视化验证结果
    """
    # 提取数据
    model_names = sorted(set(result['Model Name'] for result in results))
    classes = sorted(set(result['Class'] for result in results))
    precision = {model: [] for model in model_names}
    recall = {model: [] for model in model_names}
    f1_score = {model: [] for model in model_names}
    support = {model: [] for model in model_names}

    for result in results:
        model = result['Model Name']
        precision[model].append(float(result['Precision']))
        recall[model].append(float(result['Recall']))
        f1_score[model].append(float(result['F1-Score']))
        support[model].append(int(float(result['Support'])))

    # 绘制条形图
    metrics = ['Precision', 'Recall', 'F1-Score']
    data = {
        'Model': [],
        'Metric': [],
        'Value': []
    }

    for model in model_names:
        for metric in metrics:
            if metric == 'Precision':
                values = precision[model]
            elif metric == 'Recall':
                values = recall[model]
            elif metric == 'F1-Score':
                values = f1_score[model]
            for value in values:
                data['Model'].append(model)
                data['Metric'].append(metric)
                data['Value'].append(value)

    df = pd.DataFrame(data)

    # 绘制条形图
    plt.figure(figsize=(10, 6))
    sns.barplot(x='Model', y='Value', hue='Metric', data=df)
    plt.title('Model Performance')
    plt.xlabel('Model')
    plt.ylabel('Value')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Metric')
    plt.tight_layout()

    # 保存图表
    plt.savefig(PLOT_FILE)
    print(f"图表已保存到 {PLOT_FILE}")

    # 显示图表
    plt.show()

if __name__ == "__main__":
    # 读取 CSV 文件中的结果
    results = read_results_from_csv(RESULTS_FILE)
    print(f"读取到 {len(results)} 条验证结果。")

    # 可视化结果
    visualize_results(results)