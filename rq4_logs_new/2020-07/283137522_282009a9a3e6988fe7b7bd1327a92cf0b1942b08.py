import pandas as pd
import numpy as np
import sklearn
from sklearn import linear_model
from sklearn.utils import shuffle
import matplotlib.pyplot as pyplot
import pickle
from matplotlib import style

style.use("dark_background")

data = pd.read_csv("student-mat.csv", sep=";")
# print(data.head())

data = data[["G1", "G2", "G3", "studytime", "failures", "absences"]]
data = shuffle(data)   # shuffle the data
# print(data.head())

predict = "G3"     # label

X = np.array(data.drop([predict], 1))
Y = np.array(data[predict])
x_train, x_test, y_train, y_test = sklearn.model_selection.train_test_split(X, Y, test_size=0.1, random_state=20)

# Train model multiple time for best score
'''best_score = 0     # To check for the best accuracy of the model
for _ in range(100):
    x_train, x_test, y_train, y_test = sklearn.model_selection.train_test_split(X, Y, test_size=0.1)

    linear = linear_model.LinearRegression()

    linear.fit(x_train, y_train)         # Training the model
    acc = linear.score(x_test, y_test)   # Accuracy of the linear model
    print(acc)

    if acc > best_score:
        best_score = acc
        with open("studentmodel.pickle", "wb") as f:   # Saving our model
            # Save Model
            pickle.dump(linear, f)'''

# Load Model
pickle_in = open("studentmodel.pickle", "rb")
linear = pickle.load(pickle_in)

print("----------------------------------")
print("Coefficient: \n", linear.coef_)
print("Intercept: \n", linear.intercept_)
print("----------------------------------")

predictions = linear.predict(x_test)     # Predicting the G3 ono text data

for x in range(len(predictions)):
    print("Predictions: ", round(predictions[x]), "Data: ", x_test[x], "Actual: ", y_test[x])

# Drawing and plotting model
p = "G1"
pyplot.scatter(data[p], data[predict])
pyplot.xlabel(p)
pyplot.ylabel("Final Grade")
pyplot.show()