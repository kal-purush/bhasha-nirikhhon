###########################################
# 忽略相关的warning
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="matplotlib")
#
from IPython import get_ipython

get_ipython().run_line_magic('matplotlib', 'inline')
###########################################

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pandas as pd


def cluster_results(reduced_data, preds, centers):
    # 只取前两个维度
    reduced_data = pd.DataFrame(reduced_data[:, :2], columns=['Dimension 1', 'Dimension 2'])

    predictions = pd.DataFrame(preds, columns=['Cluster'])

    # 将数据与聚类结果合并
    plot_data = pd.concat([predictions, reduced_data], axis=1)

    fig, ax = plt.subplots(figsize=(14, 8))

    # 色彩
    cmap = cm.get_cmap('gist_rainbow')

    # 将点上色
    for i, cluster in plot_data.groupby('Cluster'):
        cluster.plot(ax=ax, kind='scatter', x='Dimension 1', y='Dimension 2',
                     color=cmap((i) * 1.0 / (len(centers) - 1)), label='Cluster %i' % (i), s=30)

    # 画出聚类中心点
    for i, c in enumerate(centers):
        ax.scatter(x=c[0], y=c[1], color='white', edgecolors='black',
                   alpha=1, linewidth=2, marker='o', s=200)
        ax.scatter(x=c[0], y=c[1], marker='$%d$' % (i), alpha=1, s=100)

    # 设置题目
    ax.set_title("cluster_results")


def real_results(reduced_data, preds):
    reduced_data = pd.DataFrame(reduced_data[:, :2], columns=['Dimension 1', 'Dimension 2'])

    predictions = pd.DataFrame(preds, columns=['Cluster'])
    plot_data = pd.concat([predictions, reduced_data], axis=1)

    fig, ax = plt.subplots(figsize=(14, 8))

    # 色彩图
    cmap = cm.get_cmap('gist_rainbow')

    # 将点上色
    for i, cluster in plot_data.groupby('Cluster'):
        cluster.plot(ax=ax, kind='scatter', x='Dimension 1', y='Dimension 2',
                     color=cmap((i) * 1.0 / (1)), label='Cluster %i' % (i), s=30)

    # 设置题目
    ax.set_title("real_results")