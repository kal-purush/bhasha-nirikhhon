import pandas as pd
import numpy as np
import matplotlib as plt

# mnist = fetch_openml('mnist_784')
mnist = pd.read_csv("mnist_784.csv")


x , y = mnist.loc[:,'pixel1':'pixel784'],mnist["class"]



# matplotlib to show image
import matplotlib.pyplot as plt

some_digit = x.iloc[4997]


some_digit_image = some_digit.values.reshape(28,28)

plt.imshow(some_digit_image,cmap=plt.cm.binary,interpolation="nearest")
plt.show()

#TRAIN TEST SPLIT

train_features = x.iloc[:4000]
train_label = y.iloc[:4000]


test_feature = x.iloc[4000:]
test_label = y.iloc[4000:]


# SHUFFLING
shuffle_index = np.random.permutation(4000)
train_features , train_label = train_features.iloc[shuffle_index] , train_label.iloc[shuffle_index]

#create a 2(two) Detector

train_features_2 = (train_features == 2)
train_label_2 = (train_label == 2)


# MODEL SELECTION

from sklearn.linear_model import LogisticRegression
clf = LogisticRegression()

clf.fit(train_features,train_label_2)

clf.predict([some_digit])

#CROSS VALIDATION

from sklearn.model_selection import cross_val_score
scores = cross_val_score(clf,train_features,train_label_2,scoring='accuracy')

print(scores.mean())

# Cross Validation Predict

from sklearn.model_selection import cross_val_predict
scores_pred=cross_val_predict(clf,train_features,train_label_2,cv=3)
print(scores_pred)

# Cross Validation(give theresold Values)

scores_pred2 = cross_val_predict(clf,train_features,train_label_2,cv=3,method="decision_function")

print(scores_pred2)

#CALCULATING CONFUSION MATRIX

from sklearn.metrics import confusion_matrix
confusion_matrix(train_label_2,scores_pred)

#PRECISION AND RECALL

from sklearn.metrics import precision_score,recall_score,f1_score
precision_score = (train_label_2,scores_pred2)
print(precision_score)

recall_score = (train_label_2,scores_pred2)
print(recall_score)

f1_score = (train_label_2,scores_pred2)
print(f1_score)

#PRECISION RECALL VS THERESHOLD CURVE


from sklearn.metrics import precision_recall_curve
# precision = precision_recall_curve(train_label_2)
# recall = precision_recall_curve(scores_pred)
# theresold = precision_recall_curve(scores_pred2)
precision,recalls,theresolds =precision_recall_curve(train_label_2,scores_pred2)


# precision.shape
# recalls.shape
print(theresolds.shape)


# FINAL RESULT

plt.plot(theresolds,precision[:-1],"b--",label="precisions")  # precision and recall me 1 value jyada hain
plt.plot(theresolds,recalls[:-1],"g-",label="recalls")
plt.xlabel("theresold")
plt.legend(loc="upper left")   #location
# plt.ylim([0,1])
plt.show()