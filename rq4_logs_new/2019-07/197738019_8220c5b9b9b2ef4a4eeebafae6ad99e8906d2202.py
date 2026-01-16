import keras
from keras.optimizers import Adam
from sklearn.model_selection import train_test_split
import rainUtil

# model = keras.models.Sequential([
#
#     keras.layers.Dense(128, input_shape=(23,), activation='relu'),
#     keras.layers.BatchNormalization(),
#     keras.layers.Dropout(0.5),
#
#     keras.layers.Dense(128, activation='relu'),
#     keras.layers.BatchNormalization(),
#     keras.layers.Dropout(0.5),
#
#     keras.layers.Dense(128, activation='relu'),
#     keras.layers.BatchNormalization(),
#     keras.layers.Dropout(0.5),
#
#     keras.layers.Dense(128, activation='relu'),
#     keras.layers.BatchNormalization(),
#     keras.layers.Dropout(0.5),
#
#     keras.layers.Dense(125, activation='relu'),
#     keras.layers.BatchNormalization(),
#     keras.layers.Dropout(0.5),
#
#     keras.layers.Dense(1, activation='sigmoid')
# ])
#
X, y = rainUtil.offerData("./weatherAUS.csv")
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size = 0.2)
X_val, X_test, y_val, y_test = train_test_split(X_val, y_val, test_size = 0.5)
# model.compile(loss='binary_crossentropy',
#               optimizer=Adam(0.00001),
#               metrics=['acc'])
#
#
# model.fit(X_train, y_train,
#                     epochs=150,
#                     validation_data=(X_val, y_val),
#                     verbose=1,
#                    )

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn import svm
import time

t0=time.time()
clf_logreg = LogisticRegression(random_state=0)
clf_logreg.fit(X_train,y_train)
y_pred = clf_logreg.predict(X_test)
score = accuracy_score(y_test,y_pred)
print('LRAccuracy :',score)
print('Time taken :' , time.time()-t0)

clf_rf = RandomForestClassifier()
clf_rf.fit(X_train,y_train)
y_pred = clf_rf.predict(X_test)
score = accuracy_score(y_test,y_pred)
print('RFAccuracy :',score)
print('Time taken :' , time.time()-t0)

t0=time.time()
clf_dt = DecisionTreeClassifier(random_state=0)
clf_dt.fit(X_train,y_train)
y_pred = clf_dt.predict(X_test)
score = accuracy_score(y_test,y_pred)
print('DTAccuracy :',score)
print('Time taken :' , time.time()-t0)

t0=time.time()
clf_svc = svm.SVC(kernel='linear')
clf_svc.fit(X_train,y_train)
y_pred = clf_svc.predict(X_test)
score = accuracy_score(y_test,y_pred)
print('Accuracy :',score)
print('Time taken :' , time.time()-t0)