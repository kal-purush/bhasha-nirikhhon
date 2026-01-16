from data_util import offer_one_day_data
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
import numpy as np

def random_forest(url):
    #获取数据
    X,y = offer_one_day_data(url)

    #reshape，处理成sklearn能识别的格式
    X = np.array(X)
    y = np.array(y)

    X.reshape((X.shape[0],-1))
    y.reshape(-1)

    #标准化转换
    scaler=StandardScaler()
    scaler.fit(X)
    X = scaler.fit_transform(X)

    #拆分数据
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.33, random_state = 42)

    #获取模型
    model = RandomForestClassifier(n_estimators=10)

    #训练模型
    model.fit(X_train,y_train)
    prediction = model.predict(X_test)

    #预测数据
    prediction = np.array(prediction)
    y_test = np.array(y_test)

    print("random forest acc : {}".format(np.mean(y_test==prediction)))

def deccision_tree(url):
    #获取数据
    X,y = offer_one_day_data(url)

    #reshape，处理成sklearn能识别的格式
    X = np.array(X)
    y = np.array(y)

    X.reshape((X.shape[0],-1))
    y.reshape(-1)

    #标准化转换
    scaler=StandardScaler()
    scaler.fit(X)
    X = scaler.fit_transform(X)

    #拆分数据
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.33, random_state = 42)

    #获取模型
    model = DecisionTreeClassifier()

    #训练模型
    model.fit(X_train,y_train)
    prediction = model.predict(X_test)

    #预测数据
    prediction = np.array(prediction)
    y_test = np.array(y_test)

    print("decision tree acc : {}".format(np.mean(y_test==prediction)))



if __name__ == '__main__':
    url = r"D:/weatherAUS.csv"
    deccision_tree(url)
    random_forest(url)
