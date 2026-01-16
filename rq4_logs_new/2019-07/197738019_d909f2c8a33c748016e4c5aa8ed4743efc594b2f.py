import re

def read_op_data(url):
    '''
    输入原始数据，清洗数据并输出
    :param url:
    :return:
    '''
    with open(url,'r') as f:
        data = []
        # lines = f.readline() #读第一行，删掉表头
        while True:
            lines = f.readline()  # 整行读取数据
            if not lines:
                break

            #处理一行的数据
            dataline=[]
            texts = lines.split(' ')

            #匹配本行过滤字段中无意义的符号
            for text in texts:
                if text==' ' or text=='':
                    continue
                else:
                    result = re.match(r"[0-9]*.?[0-9]*", text)
                    try:
                        dataline.append(float(result.group()))
                    except:
                        print(result)

            #删除无意义的字段
            process_dataline=[]
            for i in range(len(dataline)):
                if i not in (0,1,2,4,6,8,10,12,14):
                    process_dataline.append(dataline[i])
            data.append(process_dataline)

        return data



def offer_data(url,X_length,interval,y_length):
    '''

    :param url: 读取的文件的路径
    :param X_length: 输入的参数。希望通过多长的数据进行预测
    :param interval: 希望预测多少天之后的天气
    :param y_length: 希望输出多长日期的天气。

    期望输出是[interval,y_length+interval]天的天气

    :return: X,y
    '''
    texts = read_op_data(url)

    X = []
    y = []

    # 试图使用7天之前的数据来预测平均温度
    for i in range(len(texts)):

        X_tmp = []
        y_tmp = []
        if i + y_length + interval< len(texts) and i + X_length <len(texts):
            for j in range(i,i+X_length):
                X_tmp.append(texts[j])
            for j in range(i+interval,i+interval+y_length):
                y_tmp.append(texts[j][0])
            X.append(X_tmp)
            y.append(y_tmp)

    return X,y


if __name__ == '__main__':
    url = r"D:\010010 1987-2018.txt"
    X,y=offer_data(url,7,0,7)
    print(len(X))
    for text in X:
        print(text)
    for text in y:
        print(text)


