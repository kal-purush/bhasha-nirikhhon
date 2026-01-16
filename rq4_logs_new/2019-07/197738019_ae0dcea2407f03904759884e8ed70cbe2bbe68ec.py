import numpy as np
import re
import matplotlib.pyplot as plt
from sklearn import linear_model
from sklearn.model_selection import train_test_split
def readOp(url):
    with open(url,'r') as f:
        data = []
        while True:
            i=0
            lines = f.readline()  # 整行读取数据
            i=i+1
            if not lines:
                break
            if(i!=0):
                dataline=[]
                lines = f.readline()
                texts = lines.split(' ')
                # print(text)
                # break
                for text in texts:
                    if text==' ' or text=='':
                        continue
                    else:
                        result = re.match(r"[0-9]*.?[0-9]*", text)
                        try:
                            dataline.append(float(result.group()))
                        except:
                            print(result)
                process_dataline=[]
                for i in range(len(dataline)):
                    if i not in (0,1,2,4,6,8,10,12,14):
                        process_dataline.append(dataline[i])
                data.append(process_dataline)
        return data

def linerRegressionForPredict(data, predict_index, test_ratio): #data是从op得到的数据 predict_index是我们需要预测的标签（如0是预测温度） test_ratio是测试集的比例
    X = data[0:-2]
    y_temp = data[1:-1]
    y = []
    for i in range(len(y_temp)):
        y.append(y_temp[i][predict_index])
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_ratio, random_state=100) #分割训练集和测试集
    linreg = linear_model.LinearRegression()
    model = linreg.fit(X_train, y_train)
    y_pred = linreg.predict(X_test)

    sum_mean = 0
    for i in range(len(y_pred)):        #计算误差，这里使用的是均方根误差(Root Mean Squared Error, RMSE)
        sum_mean += (y_pred[i] - y_test[i]) ** 2
    sum_error = np.sqrt(sum_mean / len(y_pred))

    plt.figure()
    plt.plot(range(len(y_pred)), y_pred, 'b', label="predict")
    plt.plot(range(len(y_pred)), y_test, 'r', label="correct")
    plt.legend(loc="upper right")  # 显示图中的标签
    plt.xlabel("day")
    plt.ylabel('temperature')
    plt.show()

    return sum_error



data = readOp("010330-99999-2019.op")
print(linerRegressionForPredict(data, 0, 0.1))