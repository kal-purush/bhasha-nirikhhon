import pandas as pd
import joblib
import os


features = [
    "Fwd Seg Size Min",
    "Init Bwd Win Byts",
    "Init Fwd Win Byts",
    "Fwd Seg Size Min",
    "Fwd Pkt Len Mean",
    "Fwd Seg Size Avg",
    "Label",
    "Timestamp",
]


dtypes = {
    "Fwd Pkt Len Mean": "float",
    "Fwd Seg Size Avg": "float",
    "Init Fwd Win Byts": "int",
    "Init Bwd Win Byts": "int",
    "Fwd Seg Size Min": "int",
    "Label": "str",
}
date_columns = ["Timestamp"]

df = pd.read_csv(
    "C:\\Backup\\PycharmProjects\\PycharmProjects\\DDOS\\ddos_dataset.csv",
    usecols=features,
    dtype=dtypes,
    parse_dates=date_columns,
    index_col=None,
)

df2 = df.sort_values("Timestamp")

df3 = df2.drop(columns=["Timestamp"])

df3['Label'] = df3.Label.map({ 'ddos': 1,
       'Benign': 0,
       })


l = len(df3.index)
train_df = df3.head(int(l * 0.8))
test_df = df3.tail(int(l * 0.2))

from collections import Counter

print(Counter(train_df["Label"].values))
print(Counter(test_df["Label"].values))


y_train = train_df.pop("Label").values
y_test = test_df.pop("Label").values

X_train = train_df.values
X_test = test_df.values

from sklearn.ensemble import RandomForestClassifier
clf = RandomForestClassifier(n_estimators=50)
clf.fit(X_train, y_train)

path = 'C:\\Backup\\PycharmProjects\\PycharmProjects\\DDOS'


joblib.dump(clf,os.path.join(path, 'ddos_clf_modelV2.pkl') )


clf.score(X_train, y_train)

clf.score(X_test, y_test)

json = {"Fwd Pkt Len Mean":"233.75",
 "Fwd Seg Size Avg":"233.75",
 "Init Fwd Win Byts":"-1",
 "Init Bwd Win Byts":"211",
 "Fwd Seg Size Min":"0"
}

ddosInput = pd.DataFrame([json])

x = ddosInput[[ 'Fwd Pkt Len Mean','Fwd Seg Size Avg',
               'Init Fwd Win Byts', 'Init Bwd Win Byts',
               'Fwd Seg Size Min'
                  ]]


MODEL_FILE = 'C:\\Backup\\PycharmProjects\\PycharmProjects\\DDOS\\ddos_clf_modelV2.pkl'
ddos_estimator = joblib.load(MODEL_FILE)

prediction = ddos_estimator.predict(x)
print (prediction)