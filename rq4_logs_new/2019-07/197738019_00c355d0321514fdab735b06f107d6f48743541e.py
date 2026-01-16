import numpy as np
import re
import matplotlib.pyplot as plt
from sklearn import linear_model
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.datasets import make_blobs
from sklearn.ensemble import RandomForestClassifier
import sklearn.ensemble as ensemble
from sklearn.tree import DecisionTreeClassifier
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
def randomTreeTest(data, predict_index, test_ratio):
    X = data[0:-2]
    y_temp = data[1:-1]
    y = []
    for i in range(len(y_temp)):
        y.append(y_temp[i][predict_index])
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_ratio, random_state=100)  # 分割训练集和测试集
    nTreeList = range(15, 150, 15)
    for iTrees in nTreeList:
        #     depth=6
        depth = 3  # ----☆☆☆☆

        maxFeat = 2  # try tweaking

        wineRFModel = ensemble.RandomForestRegressor(n_estimators=iTrees, max_depth=depth, max_features=maxFeat,
                                                     oob_score=False)

        wineRFModel.fit(X_train, y_train)
        prediction = wineRFModel.predict(X_test)
    sum_mean = 0
    for i in range(len(prediction)):        #计算误差，这里使用的是均方根误差(Root Mean Squared Error, RMSE)
        sum_mean += (prediction[i] - y_test[i]) ** 2
    sum_error = np.sqrt(sum_mean / len(prediction))

    plt.figure()
    plt.plot(range(len(y_test)), prediction, 'b', label="predict")
    plt.plot(range(len(y_test)), y_test, 'r', label="correct")
    plt.legend(loc="upper right")  # 显示图中的标签
    plt.xlabel("day")
    plt.ylabel('temperature')
    plt.show()
    print(sum_error)




data = readOp("010330-99999-2019.op")
print(randomTreeTest(data, 0, 0.1))