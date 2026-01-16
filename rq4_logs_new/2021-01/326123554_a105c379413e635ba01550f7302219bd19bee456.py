# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import collections
import pandas as pd
from matplotlib import pyplot as plt
import time
import xlrd
from wordcloud import WordCloud


# Press the green button in the gutter to run the script.
def show_times():
    # 读取数据
    df = pd.read_excel("D:/films.xlsx")
    # print(type(df))    # <class 'pandas.core.frame.DataFrame'>

    show_time = list(df["上映时间"])
    # 有上映时间数据是1961(中国大陆)这样的  处理一下  字符串切片
    show_time = [str(s)[0:4] for s in show_time]

    show_time_count = collections.Counter(show_time)

    # 取数量最多的前10  得到一个列表  里面每个元素是元组
    # (年份, 数量)
    show_time_count = show_time_count.most_common(10)
    # 字典推导式
    show_time_dic = {k: v for k, v in show_time_count}

    # 按年份排序
    show_time = sorted(show_time_dic)
    # 年份对应高分电影数量
    counts = [show_time_dic[k] for k in show_time]

    plt.figure(figsize=(9, 6), dpi=100)
    # 设置字体
    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']

    # 绘制条形图
    plt.bar(show_time, counts, width=0.5, color="paleturquoise")

    # y轴刻度重新设置一下
    plt.yticks(range(0, 16, 2))

    # 添加描述信息
    plt.xlabel("年份")
    plt.ylabel("高分电影数量")
    plt.title("上映高分电影数量最多的年份Top10", fontsize=15)

    # 添加网格  网格的透明度  线条样式
    plt.grid(alpha=0.2, linestyle=":")

    plt.show()


def rate_score():
    # 读取数据
    df = pd.read_excel("D:/films.xlsx")

    # 豆瓣电影Top250  排名  评分  散点图   描述关系
    rating = list(df["排名"])
    rating_score = list(df["评分"])


    plt.figure(figsize=(9, 6), dpi=100)
    # 设置字体
    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']

    # 绘制散点图  设置点的颜色
    plt.scatter(rating_score, rating, c='cadetblue')

    # 添加描述信息  设置字体大小
    plt.xlabel("评分", fontsize=12)
    plt.ylabel("排名", fontsize=12)
    plt.title("豆瓣电影Top250评分-排名的散点分布", fontsize=15)

    # 添加网格  网格的透明度  线条样式
    plt.grid(alpha=0.5, linestyle=":")

    plt.savefig('test2.PNG')
    plt.show()


def type_cloud():
    # 读取数据
    data = xlrd.open_workbook('D:/films.xlsx')
    table = data.sheets()[0]
    type_list = []
    for i in range(1, table.nrows):
        x = table.row_values(i)
        genres = x[7].split("/")
        for j in genres:
            type_list.append(j)

    type_count = collections.Counter(type_list)

    # 绘制词云
    my_wordcloud = WordCloud(
        max_words=100,           # 设置最大显示的词数
        font_path='simhei.ttf',  # 设置字体格式
        max_font_size=66,        # 设置字体最大值
        random_state=30,         # 设置随机生成状态，即多少种配色方案
        min_font_size=12,        # 设置字体最小值
    ).generate_from_frequencies(type_count)

    # 显示生成的词云图片
    plt.imshow(my_wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.savefig('test3.PNG')
    plt.show()


def rate_country():
    df = pd.read_excel('D:/films.xlsx')
    area = list(df['上映地区'])
    sum_area = []
    for x in area:
        x = x.split(" / ")
        for i in x:
            sum_area.append(i)

    area_count = collections.Counter(sum_area)
    area_dic = dict(area_count)
    area_count = [(k, v) for k, v in list(area_dic.items())]
    # 按国家或地区上榜电影数量排序
    area_count.sort(key=lambda k: k[1])
    # 取国家或地区上榜电影数量最多的前十
    area = [m[0] for m in area_count[-10:]]
    nums = [m[1] for m in area_count[-10:]]


    plt.figure(figsize=(9, 6), dpi=100)
    # 设置字体
    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
    # 绘制横着的条形图
    plt.barh(area, nums, color='red')

    # 添加描述信息
    plt.xlabel('电影数量')
    plt.title('国家或地区上榜电影数量最多的Top10')

    plt.savefig('test4.PNG')
    plt.show()


def votenum_rank():
    df = pd.read_excel('D:/films.xlsx')
    name = list(df['电影名'])
    ranting_num = list(df['评价人数'])
    # (电影名, 评价人数)
    info = [(m, int(n)) for m, n in list(zip(name, ranting_num))]
    # 按评价人数排序
    info.sort(key=lambda x: x[1])
    # print(info)

    name = [x[0] for x in info[-10:]]
    ranting_num = [x[1] for x in info[-10:]]

    plt.figure(figsize=(12, 6), dpi=100)
    # 设置字体
    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']

    # 绘制横着的条形图
    plt.barh(name, ranting_num, color='cyan', height=0.4)

    # 添加描述信息
    plt.xlabel('评价人数')
    plt.title('豆瓣电影Top250-评价人数Top10')

    plt.savefig('test5.PNG')
    plt.show()


if __name__ == '__main__':
    show_times()
    rate_score()
    type_cloud()
    rate_country()
    votenum_rank()
# See PyCharm help at https://www.jetbrains.com/help/pycharm/