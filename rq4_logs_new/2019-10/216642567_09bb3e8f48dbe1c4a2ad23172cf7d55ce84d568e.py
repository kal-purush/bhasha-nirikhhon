import json
import requests
import pandas as pd
import os
import re
import jieba
import jieba.analyse
import datetime
# from collections import Counter
# import numpy
from pyecharts import options as opts
from pyecharts.charts import Calendar, Pie, Bar, Map, Geo
from pyecharts.globals import SymbolType, ThemeType, GeoType
from pyecharts.charts import Page, WordCloud, TreeMap

'''
author: BW
vision: 1.1
'''

'''
=============================================爬取数据==========================================================
'''
def get_resou_data(begin_date, end_data):
    '''
    数据爬取
    :return resou
    '''
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win32; x32; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Connection': 'keep-alive'
        }
    cookie = {'v': '3; iuuid=1A6E888B4A4B29B16FBA1299108DBE9CDCB327A9713C232B36E4DB4FF222CF03; webp=true; ci=1%2C%E5%8C%97%E4%BA%AC; __guid=26581345.3954606544145667000.1530879049181.8303; _lxsdk_cuid=1646f808301c8-0a4e19f5421593-5d4e211f-100200-1646f808302c8; _lxsdk=1A6E888B4A4B29B16FBA1299108DBE9CDCB327A9713C232B36E4DB4FF222CF03; monitor_count=1; _lxsdk_s=16472ee89ec-de2-f91-ed0%7C%7C5; __mta=189118996.1530879050545.1530936763555.1530937843742.18'}

    resou = pd.DataFrame(columns=['date','title','searchCount','rank']) 
    resou_date = get_between_day(begin_date, end_data)     
    print('=====正在爬取对应日期的数据=====')
    for i in resou_date:   # i日期
        print(i)
        url= 'https://www.enlightent.cn/research/top/getWeiboHotSearchDayAggs.do?date={}'.format(str(i))
        html = requests.get(url=url, cookies=cookie, headers=header).content
        data = json.loads(html.decode('utf-8'))
        for j in range(100):   # 每天爬取前100条热搜标题
            resou = resou.append(
                {'date': i, 'title': data[j]['keyword'], 'searchCount': data[j]['searchCount'], 'rank': j+1}, 
                ignore_index=True
                )
    print('=====要求日期的数据爬取完成=====')
    return resou


def get_between_day(begin_date, end_date):
    '''
    :param begin_date 开始日期
    :param end_date 结束日期
    :return date_list 
    '''
    date_list = []
    between_date = pd.date_range(begin_date, end_date)
        # DatetimeIndex(['2019-10-01', '2019-10-02', '2019-10-03'], dtype='datetime64[ns]', freq='D')
        # <class 'pandas.core.indexes.datetimes.DatetimeIndex'>
    for date in between_date:    
        date_str = date.strftime("%Y/%m/%d")    # datetime 转 str: strftime()
        date_list.append(date_str)
    return date_list 


'''
=============================================将数据保存为excel==========================================================
'''
def save_excel_data(resou, excel_path):
    '''
    将标题分词，并保存
    :param resou
    '''
    resou_wl = resou.apply(add_words_list, axis=1)   # 按行 apply 第四个自定义函数 add_words_list() 到 resou
    resou_wl.to_excel(excel_path)                # pandas.DataFrame.to_excel()    生成文件'热搜数据.xlsx'


def add_words_list(df):
    '''
    分词
    :param df
    :return df 新添加"words_list"，为每个title中出现单词的列表
    '''
    df['words_list'] = []
    word_generator = jieba.cut_for_search(df['title'])
    for word in word_generator:
        df['words_list'].append(word)
    return df

'''
=============================================数据可视化==========================================================
'''
def draw_calendar(resou, begin_date, end_data):
    '''
    绘制日历图
    :param resou
    '''
    resou['searchCount'] = resou['searchCount'].apply(int)   # 爬取的热度为字符串，转化为整数
    resou_dt = resou.groupby('date', as_index=False).agg({'searchCount':['mean']})   # 按日期分组，再聚合得到每日热搜数量均值
    resou_dt.columns = ['date','avg_count']
    data = [
        [resou_dt['date'][i], resou_dt['avg_count'][i]]
        for i in range(resou_dt.shape[0])
        ]
    calendar = (
        Calendar(init_opts=opts.InitOpts(width='1800px',height='1500px'))
            .add("", data,calendar_opts=opts.CalendarOpts(range_=[begin_date, end_data])) 
            .set_global_opts(
                title_opts=opts.TitleOpts(title="2019每日热搜平均指数",pos_left='15%'),
                visualmap_opts=opts.VisualMapOpts(
                    max_=3600000,
                    min_=0,
                    orient="horizontal",
                    is_piecewise=False,
                    pos_top="230px",
                    pos_left="100px",
                    pos_right="10px"
                )
            )
            .render('日期热力图.html')     
        )


def draw_word_cloud(resou):
    '''
    绘制词云       没有用到searchCount，即没有用到热搜数，仅仅与上榜天数有关   单词
    :param resou
    '''
    title_string = ' '.join(resou['title'])
    keywords_count_list_TR = jieba.analyse.textrank(title_string, topK=50, withWeight=True)
    keywords_count_list_TI = jieba.analyse.extract_tags(title_string, topK=50, withWeight=True)
    print(keywords_count_list_TR, keywords_count_list_TR)
    word_cloud_TR = (
        WordCloud(init_opts=opts.InitOpts(theme=ThemeType.PURPLE_PASSION))
            .add("", keywords_count_list_TR, word_size_range=[20, 50], shape='star')
            .set_global_opts(title_opts=opts.TitleOpts(title="微博热搜词云TOP50", subtitle="基于TextRank算法的关键词抽取"))   
            .render('Weibo_WordCloud_TR.html')
    )
    word_cloud_TI = (
        WordCloud(init_opts=opts.InitOpts(theme=ThemeType.CHALK))
            .add("", keywords_count_list_TI, word_size_range=[20, 100], shape='triangle')
            .set_global_opts(title_opts=opts.TitleOpts(title="微博热搜词云TOP50", subtitle="基于TF-IDF算法的关键词抽取"))
            .render('Weibo_WordCloud_TI.html')
    )
    print('=====词云绘制完毕=====')


def fuzzy_match_title(resou, key_word):
    '''
    匹配词的热搜标题统计   基于searchCount，与上榜天数次数无关    是标题不是单词，当然可以用上面方法形成单词，但没意义，因为主题key_word已定
    :param resou
    :param key_word    若是多个关键词，格式 key_word1|key_word2|key_word3​
    '''
    # 预处理热评
    resou['searchCount'] = resou['searchCount'].apply(int)
    # .str.contains()模糊匹配 
    word_title = resou[resou['title'].str.contains(key_word)]   # DataFrame类   
    # 处理数据  如果一个热搜上了两天，取热度高的那一次
    word_title = word_title.groupby(['title'],as_index=False).agg({'searchCount':['max']}) 
    # 重命名列名 
    word_title.columns = ['title', 'count']   # DataFrame类 
    return word_title

def fuzzy_match_title_cloud(resou, key_word):
    '''
    标题云
    '''
    # 预处理标题
    words = re.split("\|", key_word)
    word = 'or'.join(words)    # 标题不能出现 | 
    word_title = fuzzy_match_title(resou, key_word)   
    data = [(word_title['title'][i], word_title['count'][i]/1000000) for i in range(word_title.shape[0])]  # word_title.shape[0]行数   # list类 形如 [('title1', float1), ('title2', float2), ...]   /1000000 必须​
    wc = (
        WordCloud(init_opts=opts.InitOpts(theme=ThemeType.ROMA))
            .add("", data, word_size_range=[20, 50], shape='pentagon')
            .set_global_opts(title_opts=opts.TitleOpts(title='关于{}的热搜标题云.html'.format(word), subtitle='基于searchCount'))
            .render('关于“{}”的热搜标题云.html'.format(word))
        )
    print('=====关于“{}”的热搜标题云完成====='.format(word))

def fuzzy_match_title_bar(resou, key_word):
    '''
    标题柱状图
    '''
    words = re.split("\|", key_word)
    word = 'or'.join(words)    # 标题不能出现 | 
    word_title = fuzzy_match_title(resou, key_word)
    word_sort = word_title.sort_values(by='count', ascending=False)
    word_sort_top20 = word_sort[:20]
    data_list = word_sort_top20.values.tolist()       # [ ['title1', count1], ['title2',counr2], ... ]
    x_data = []
    y_data = []
    for data in data_list:
        x = data[0]
        x_data.append(x)
        y = data[1]
        y_data.append(y)
    x_data.reverse()
    y_data.reverse()
    bar = (
        Bar(init_opts=opts.InitOpts(theme=ThemeType.LIGHT))
            .add_xaxis(x_data)             
            .add_yaxis("", y_data)   
            .reversal_axis()                                                        # xy轴互换
            .set_series_opts(label_opts=opts.LabelOpts(position="right"))           # 数据相对于柱子的位置
            .set_global_opts(
                title_opts=opts.TitleOpts(title="热搜TOP20"),
                yaxis_opts=opts.AxisOpts(name="热搜标题"),
                xaxis_opts=opts.AxisOpts(name="热搜指数")    
                )
            .render('关于“{}”的热搜标题柱状图.html'.format(word))
        )
    print('=====关于“{}”的热搜标题柱状图完成====='.format(word))


def fuzzy_match_title_pie(resou, key_word):
    '''
    标题饼图、玫瑰图
    '''
    words = re.split("\|", key_word)
    word = 'or'.join(words)    # 标题不能出现 | 
    word_title = fuzzy_match_title(resou, key_word)
    word_sort = word_title.sort_values(by='count', ascending=False)
    word_sort_top20 = word_sort[:20]
    data_list = word_sort_top20.values.tolist()       # [ ['title1', count1], ['title2',counr2], ... ]
    x_data = []
    y_data = []
    for data in data_list:
        x = data[0]
        x_data.append(x)
        y = data[1]
        y_data.append(y)
    pie = (
        Pie()
            .add("", [list(z) for z in zip(x_data, y_data)])
            .set_global_opts(
                title_opts=opts.TitleOpts(title="热搜TOP20"),
                legend_opts=opts.LegendOpts(type_="scroll", pos_left="80%", orient="vertical")  # 图例放左边
                )
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}: {d}%"))
            .render('关于“{}”的热搜标题饼图.html'.format(word))
        )
    pie = (
        Pie()
            .add("", [list(z) for z in zip(x_data, y_data)],
                radius=["30%", "75%"],
                center=["40%", "50%"],
                rosetype="radius")
            .set_global_opts(
                title_opts=opts.TitleOpts(title="热搜TOP20"),
                legend_opts=opts.LegendOpts(type_="scroll", pos_left="80%", orient="vertical")
                )
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}: {d}%"))
            .render('关于“{}”的热搜标题玫瑰图R.html'.format(word))
        )
    pie = (
        Pie()
            .add("", [list(z) for z in zip(x_data, y_data)],
                radius=["30%", "75%"],
                center=["40%", "50%"],
                rosetype="area")
            .set_global_opts(
                title_opts=opts.TitleOpts(title="热搜TOP20"),
                legend_opts=opts.LegendOpts(type_="scroll", pos_left="80%", orient="vertical")
                )
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}: {d}%"))
            .render('关于“{}”的热搜标题玫瑰图A.html'.format(word))
        )
    print('=====关于“{}”的热搜标题饼图、玫瑰图完成====='.format(word))


def draw_title_bar(resou):
    '''
    最热搜标题  
    :param resou
    '''
    # 预处理热评
    resou['searchCount'] = resou['searchCount'].apply(int)
    # .str.contains()模糊匹配   
    # 处理数据  如果一个热搜上了两天，取热度高的那一次
    word_title = word_title.groupby(['title'],as_index=False).agg({'searchCount':['max']}) 
    # 重命名列名 
    word_title.columns = ['title', 'count']   # DataFrame类 


def find_high_freq_word(resou): 
    '''
    :param resou
    return count_result[:20]
    '''
    # 1、预处理数据
    # # 转化为字符串处理
    title_string = ' '.join(resou['words_list']).replace('[', '').replace(']', '').replace('] [', ', ').replace("',", '').replace("'", '').replace("   ", ' ')   
    words_list = re.split(" ", title_string)   # 单词列表  ['单词1', '单词2', ...]
    # words_dict = Counter(words_list)    # 频数，返回的值是字典格式  {'xx':8,'xxx':9}
    # result = words_dict.most_common(20)  # Counter(words_list).most_common(20)  返回[('xxx', 8), ('xxx', 5),...] 前20个
    # 发现大量: 的, 被, 了... 之类的无用数据
    # 转化为series类处理，方便清洗数据
    words_series = pd.Series(words_list)
    count_result = words_series.value_counts()
    stop_word_list = [
        '的', '被', '了', '你', '是', '我', '人', '将', '和', '不', '与', '在', '有', '新', '什么', '为', '大', '后', '年', 
        '岁', '吗', '穿', '最', '向', '看', '给', '都', '吃', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '10',
        '个', '说', '好', '就', '中', '时', '或', '杨', '亿', '女', '用', '月', '一', '拍', '对', '要', '这', '已', '元', 
        '吧', '多', '去', '里', '上', '小', '搭', '易', '到', '万', '让', '超', '谁', '把', '遭', '会'
        ]
    for stop_word in stop_word_list:
        count_result.pop(stop_word)
    return count_result[:20]

def draw_high_freq_word_bar(resou):
    data = find_high_freq_word(resou)
    bar = (
        Bar(init_opts=opts.InitOpts(theme=ThemeType.ROMA))
            .add_xaxis(data.index.tolist())             
            .add_yaxis("", data.values.tolist())   
            .set_global_opts(
                title_opts=opts.TitleOpts(title="热搜单词TOP20"),
                yaxis_opts=opts.AxisOpts(name="频数"),
                xaxis_opts=opts.AxisOpts(name="热搜单词", axislabel_opts=opts.LabelOpts(rotate=-45))    
                )
            .render('热搜单词柱状图.html')
        )
    print('=====热搜单词柱状图完成=====')


def draw_high_freq_word_pie(resou):
    data = find_high_freq_word(resou)
    pie = (
        Pie()
            .add("", [list(z) for z in zip(data.index.tolist(), data.values.tolist())])
            .set_global_opts(
                title_opts=opts.TitleOpts(title="热搜TOP20"),
                legend_opts=opts.LegendOpts(type_="scroll", pos_left="80%", orient="vertical")  # 图例放左边
                )
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}: {d}%"))
            .render('热搜单词饼图.html')
        )
    pie = (
        Pie()
            .add("", [list(z) for z in zip(data.index.tolist(), data.values.tolist())],
                radius=["30%", "75%"],
                center=["40%", "50%"],
                rosetype="radius")
            .set_global_opts(
                title_opts=opts.TitleOpts(title="热搜TOP20"),
                legend_opts=opts.LegendOpts(type_="scroll", pos_left="80%", orient="vertical")
                )
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}: {d}%"))
            .render('热搜单词玫瑰图R.html')
        )
    pie = (
        Pie()
            .add("", [list(z) for z in zip(data.index.tolist(), data.values.tolist())],
                radius=["30%", "75%"],
                center=["40%", "50%"],
                rosetype="area")
            .set_global_opts(
                title_opts=opts.TitleOpts(title="热搜TOP20"),
                legend_opts=opts.LegendOpts(type_="scroll", pos_left="80%", orient="vertical")
                )
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}: {d}%"))
            .render('热搜单词玫瑰图A.html')
        )
    print('=====热搜单词饼图、玫瑰图完成=====')


def topic_bar_pie(resou, topic, w_lst):
    n_lst =[]
    w_n_lst = []
    for w in w_lst:
        n = fuzzy_match_title(resou, w).shape[0]
        n_lst.append(n)
        w_n_lst.append(tuple([w, n]))
    bar = (
        Bar(init_opts=opts.InitOpts(theme=ThemeType.DARK))
            .add_xaxis(w_lst)             
            .add_yaxis("", n_lst)   
            .reversal_axis()                                                        # xy轴互换
            .set_series_opts(label_opts=opts.LabelOpts(position="right"))           # 数据相对于柱子的位置
            .set_global_opts(
                title_opts=opts.TitleOpts(title="{}热词".format(topic)),
                yaxis_opts=opts.AxisOpts(name="频数"),
                xaxis_opts=opts.AxisOpts(name="热词")    
                )
            .render('有关“{}”的热词柱状图.html'.format(topic))
        )
    print('=====有关“{}”的热词柱状图完成====='.format(topic))
    pie = (
        Pie()
            .add("", w_n_lst)
            .set_global_opts(
                title_opts=opts.TitleOpts(title="{}热词".format(topic)),
                legend_opts=opts.LegendOpts(type_="scroll", pos_left="80%", orient="vertical")  # 图例放左边
                )
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}: {d}%"))
            .render('有关“{}”的热词饼图.html'.format(topic))
        )
    print('=====有关“{}”的热词单词饼图完成====='.format(topic))


def tree_map(resou, total_lst):
    '''
    矩形树图   最多只有两层
    '''
    t_m_lst = []
    for pair in total_lst:
        m = 0
        w_n_lst = []
        if pair[1] == []:
            m = fuzzy_match_title(resou, pair[0]).shape[0]
            t_m_lst.append({'value': m, 'name': pair[0]})
        else:
            for word in pair[1]:
                n = fuzzy_match_title(resou, word).shape[0]
                w_n_lst.append({'value': n, 'name': word})
                m += n
            t_m_lst.append({'value': m, 'name': pair[0], 'children': w_n_lst})
    tree = (
        TreeMap(init_opts=opts.InitOpts(theme=ThemeType.ESSOS))
            .add("", t_m_lst, pos_left=0, pos_right=0, pos_top=50, pos_bottom=50)
            .set_global_opts(title_opts=opts.TitleOpts(title="热搜",pos_left='center'))
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}\n\n {c}",font_size=17,     # 名字\n\n数据  大小17
                                                   color='black',position='inside',font_weight='bolder'))  # 黑色，位置在内部，粗体
            .render('矩形树图.html')
        )
    print('=====矩形树图完成=====')


def draw_heatmap(resou):
    province_captal_name = [
        '北京|京', '天津|津', '上海|沪', '重庆|渝' '浙江|浙|杭州', '安徽|皖|合肥', '福建|闽|福州', '香港', '澳门', '台湾|台北',
        '江西|赣|南昌','山东|鲁|济南', '河南|豫|郑州', '内蒙古|内蒙|呼和浩特', '湖北|鄂|武汉', '新疆|乌鲁木齐', '湖南|湘|长沙',
        '宁夏|银川', '广东|粤|广州','西藏|拉萨','海南|琼|海口','广西|桂|南宁','四川|川|蜀|成都','河北|冀|石家庄','贵州|黔|贵阳',
        '山西|晋|太原','云南|滇|昆明','辽宁|辽|沈阳','陕西|陕|秦|西安','吉林|长春', '甘肃|陇|兰州','黑龙江|哈尔滨','青海|西宁', 
        '江苏|南京'
    ]
    province_lst = [
        '北京', '天津', '上海', '重庆' '浙江', '安徽', '福建', '香港', '澳门', '台湾', '江西','山东', '河南', '内蒙古', '湖北', 
        '新疆', '湖南', '宁夏', '广东','西藏','海南','广西','四川','河北','贵州','山西','云南','辽宁','陕西','吉林', '甘肃|',
        '黑龙江','青海', '江苏'
    ]
    province_distribution = []
    for province, word in zip(province_lst, province_captal_name):
        province_distribution.append(
            (province, fuzzy_match_title(resou, word).shape[0])
            )
    resou_map = (
        Map()
            .add("热搜地图", province_distribution, "china")
            .set_global_opts(
                visualmap_opts=opts.VisualMapOpts(min_=0, max_=105),
            )
            .render(path="热搜地图.html")
    )
    resou_geo = (
        Geo()
            .add_schema(maptype="china")
            .add("热搜HEATMAP", province_distribution, type_=GeoType.HEATMAP)   # Geo 图类型，有 scatter, effectScatter, heatmap 
            .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
            .set_global_opts(
                visualmap_opts=opts.VisualMapOpts(min_=0, max_=105),
                title_opts=opts.TitleOpts(title="HEATMAP"),  
            )
            .render(path="热搜HEATMAP.html")
    )
    resou_geo = (
        Geo()
            .add_schema(maptype="china")
            .add("热搜SCATTER", province_distribution, GeoType.SCATTER)     
            .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
            .set_global_opts(
                visualmap_opts=opts.VisualMapOpts(min_=0, max_=105),
                title_opts=opts.TitleOpts(title="SCATTER"),
            )
            .render(path="热搜SCATTER.html")
    )
    resou_geo = (
        Geo()
            .add_schema(maptype="china")
            .add("热搜effectScatter", province_distribution, type_='effectScatter')
            .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
            .set_global_opts(
                visualmap_opts=opts.VisualMapOpts(min_=0, max_=105),
                title_opts=opts.TitleOpts(title="effectScatter"),
            )
            .render(path="热搜effectScatter.html")
    ) 
    print('=====热力图绘制完成=====')
    

if __name__ == '__main__':
    # # 爬取数据
    # resou_DTCR = get_resou_data('2019/01/01', '2019/10/20')     

    # # 添加分词数据列 并 保存为'热搜数据.xlsx'
    # save_excel_data(resou_DTCR, excel_path='热搜数据.xlsx')

    # 读取分词信息
    resou_WL = pd.read_excel('热搜数据.xlsx')
    
    # # 日历图
    # draw_calendar(resou_WL, '2019/01/01', '2019/10/20')    

    # # 绘制所有热搜词云      1、单词 2、没有用到热搜数，仅仅与相关标题，及其上榜天数有关
    # draw_word_cloud(resou_WL)

    # # 根据关键词搜索 并 绘制相关热搜标题云     1、标题 2、根据最高热搜数绘制    热搜标题已经是精炼信息了，词云意义不大
    # fuzzy_match_title_cloud(resou_WL, "") 
    # fuzzy_match_title_cloud(resou_WL, "结婚") 
    # fuzzy_match_title_cloud(resou_WL, "婚|恋|分手")
    # fuzzy_match_title_cloud(resou_WL, "道歉")  
    # fuzzy_match_title_cloud(resou_WL, "心疼")
    # # 热搜标题柱状图，饼图，玫瑰图
    # fuzzy_match_title_bar(resou_WL, "道歉")
    # fuzzy_match_title_bar(resou_WL, "婚|恋|分手")
    # fuzzy_match_title_pie(resou_WL, "分手") 
    # # 这里只爬取1年数据，所以引入fuzzy_match后，数据有点少，比如心疼，只有30条匹配，所以最好用热搜数这个信息


    # # 热搜单词柱状图，饼图，玫瑰图       1、单词 2、没有用到热搜数
    # draw_high_freq_word_bar(resou_WL)
    # draw_high_freq_word_pie(resou_WL)

    # # 主题型 饼图，柱状图，矩形树图     单词频数   待改进
    # topic = '婚恋主题'
    # w_lst = ["离婚", "结婚", "分手", "恋爱", "恋情"]   # 手动挑选
    # topic_bar_pie(resou_WL, topic, w_lst)
    
    # topic = '明星主题'
    # w_lst = [
    #     "周杰伦", "张艺兴", "易烊千玺", "杨超越", "吴亦凡", "王思聪", "沈腾", "翟天临", "迪丽热巴",
    #     "邓伦", "蔡徐坤", "刘昊然", "王俊凯", "王源", "鹿晗", "刘强东"
    #     ]   # 手动挑选
    # topic_bar_pie(resou_WL, topic, w_lst)

    # # 矩形树图
    # total_lst = [
    #     (
    #         '明星',
    #         ["周杰伦", "张艺兴", "易烊千玺", "杨超越", "吴亦凡", "王思聪", "沈腾", "翟天临", "迪丽热巴", "邓伦", "蔡徐坤", "刘昊然", "王俊凯", "王源", "鹿晗", "刘强东"]
    #     ),
    #     (
    #         '婚恋', 
    #         ["离婚", "结婚", "分手", "恋爱", "恋情"]
    #     ),
    #     (
    #         '中国', 
    #         ["内地", "台湾", "香港", "澳门"]
    #     ),
    #     (
    #         '美国',
    #         []
    #     )
    # ]
    # tree_map(resou_WL, total_lst)     
    
    # 热力图
    draw_heatmap(resou_WL)
    





    
    # # treemap例子
    # # 数据格式
    # data = [
    #     {"value": 40, "name": "我是A"},
    #     {
    #         "value": 180,
    #         "name": "我是B",
    #         "children": [
    #             {
    #                 "value": 76,
    #                 "name": "我是B.children",
    #                 "children": [
    #                     {"value": 12, "name": "我是B.children.a"},
    #                     {"value": 28, "name": "我是B.children.b"},
    #                     {"value": 20, "name": "我是B.children.c"},
    #                     {"value": 16, "name": "我是B.children.d"},
    #                 ],
    #             }
    #         ],
    #     },
    # ]
    # tree = (
    #     TreeMap(init_opts=opts.InitOpts(theme=ThemeType.ESSOS))
    #         .add("", data, pos_left=0, pos_right=0, pos_top=50, pos_bottom=50)
    #         .set_global_opts(title_opts=opts.TitleOpts(title="example",pos_left='center'))
    #         .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}\n\n {c}",font_size=17,color='black',position='inside',font_weight='bolder'))
    #         .render('treemap例子.html')
    #     )