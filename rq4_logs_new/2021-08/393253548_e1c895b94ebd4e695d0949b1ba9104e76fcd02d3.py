import pandas as pd
from sklearn.datasets import  load_iris
from sklearn.model_selection import train_test_split
from  sklearn.ensemble import RandomForestClassifier
from sklearn import metrics

iris = load_iris()
print(iris.keys())
print(iris.target_names)
print(iris.feature_names)

df = pd.DataFrame(iris.data, columns=iris.feature_names)
print(df)

df['target'] = iris.target
print(df)

x=df[['sepal length (cm)','sepal width (cm)','petal length (cm)','petal width (cm)']]
y=df['target']

x=df[['sepal length (cm)','sepal width (cm)','petal length (cm)','petal width (cm)']]
y=df['target']

x_train, x_test, y_train, y_test = train_test_split(x,y,test_size=0.2)

model = RandomForestClassifier(n_estimators=80)
model.fit(x_train,y_train)

y_pred=model.predict(x_test)
print(model.score(x_test,y_test))


print("Accuracy:",metrics.accuracy_score(y_test,y_pred))




