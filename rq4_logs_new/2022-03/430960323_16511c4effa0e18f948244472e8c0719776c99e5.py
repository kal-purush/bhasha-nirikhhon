import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score

dataset = pd.read_csv("IRIS.csv")
# print(dataset);

x = dataset.iloc[:, 0:4]
y = dataset.iloc[:, 4]

x_train, x_test, y_train, y_test = train_test_split(x,y,train_size=0.8)

model = SVC()
model.fit(x_train, y_train)

predict_flower = model.predict(x_test)

print("Accuracy", accuracy_score(y_test, predict_flower))



