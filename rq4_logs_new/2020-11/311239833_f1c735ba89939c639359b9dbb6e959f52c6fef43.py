import csv

import text1,text2,text3

if __name__ == '__main__':
    name = ["笔记本电脑"
    ,"平板电脑"
    ,"华为"
    ,'苹果'
    ,'小米'
    ,'vivo'
    ,'oppo'
    ,'三星'
    ,'荣耀'
    ,'魅族'
    ,'耳机'
    ,'充电宝'
    ,'数据线'
    ,'鼠标'
    ,'键盘']  #输入要爬取的名称
    for index in range(len(name)):
        good_list = text3.get_good(name[index])
        good_nameList = []
        for index1 in range(len(good_list)):
            good_nameList.append(text2.mmm(good_list[index1]["商品链接"], name[index]))
        header = ['商品名称', '商品图片', "商品链接"]  # 数据列名
        with open(name[index] + ".csv", 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=header)  # 提前预览列名，当下面代码写入数据时，会将其一一对应。
            writer.writeheader()
            writer.writerows(good_nameList)  # 写入数据

'''name = ["笔记本电脑"
,"平板电脑"
,"华为"
,'苹果'
,'小米'
,'vivo'
,'oppo'
,'三星'
,'荣耀'
,'魅族'
,'耳机'
,'充电宝'
,'数据线'
,'鼠标'
,'键盘']'''