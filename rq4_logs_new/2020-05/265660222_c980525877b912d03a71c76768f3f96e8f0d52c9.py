from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score
import numpy as np
import pandas as pd
import hashlib
from copy import deepcopy
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.ensemble import BaggingClassifier

def train_test_splitter(X, y, percent, r=3):
    X_train = deepcopy(X)
    y_train = deepcopy(y)
    X_test = pd.DataFrame()
    y_test = pd.DataFrame()
    _myset = set()
    test_range = int(percent * len(X))
    for i in range(test_range):
        v = hashlib.blake2b(str(i).encode('UTF-8'), digest_size=3).hexdigest()
        v = int(str(int(v, 16))[:r])
        while _myset.issuperset([v]):
            v = v + 1
        _myset.add(v)
        X_test = X_test.append(X.loc[[v]])
        y_test = y_test.append(y.loc[[v]])
        X_train = X_train.drop(v, axis=0)
        y_train = y_train.drop(v, axis=0)
    X_train = X_train.reset_index()
    y_train = y_train.reset_index()
    X_test = X_test.reset_index()
    y_test = y_test.reset_index()
    return X_train.drop('index', axis=1), y_train.drop('index', axis=1), X_test.drop('index', axis=1), y_test.drop(
        'index', axis=1)


def discretization(x, feature, ranges):
    data = deepcopy(x)
    for i in range(len(ranges)):
        for j in range(len(data)):
            if ranges[i][0] <= data[feature][j] < ranges[i][1]:
                data[feature][j] = ranges[i][2]
    return data


data = pd.read_csv("D:\Bachelor's final year\Second Semester\ML\project\HCV-Egy-Data.csv")
y_classes = pd.DataFrame(np.asarray(data['Baselinehistological staging']))
y_regression = pd.DataFrame(np.asarray(data['Baseline histological Grading']))
data = data.drop('Baselinehistological staging', axis=1)
data = data.drop('Baseline histological Grading', axis=1)

# discretization
age_ranges = [[0, 32, 30], [32, 37, 35], [37, 42, 40], [42, 47, 45], [47, 52, 50], [52, 57, 55], [57, 63, 60]]
data = discretization(data, 'Age', age_ranges)

ast1_ranges = [[0, 20, 10], [20, 40, 30], [40, 129, 100]]
data = discretization(data, 'AST 1', ast1_ranges)

bmi_ranges = [[0, 18.5, 15], [185, 25, 20], [25, 30, 27], [30, 35, 33], [35, 41, 37]]
data = discretization(data, 'BMI', bmi_ranges)

wbc_ranges = [[0, 4000, 2000], [4000, 11000, 8000], [11000, 12102, 10000]]
data = discretization(data, 'WBC', wbc_ranges)

rbc_ranges = [[0, 3000000, 2000000], [3000000, 5000000, 4000000], [5000000, 5018452, 500500]]
data = discretization(data, 'RBC', rbc_ranges)

plat_ranges = [[93013, 100000, 95000], [100000, 255000, 200000], [255000, 226466, 226000]]
data = discretization(data, 'Plat', plat_ranges)

data = discretization(data, 'ALT 1', ast1_ranges)
data = discretization(data, 'ALT4', ast1_ranges)
data = discretization(data, 'ALT 12', ast1_ranges)
data = discretization(data, 'ALT 24', ast1_ranges)
data = discretization(data, 'ALT 36', ast1_ranges)
data = discretization(data, 'ALT 48', ast1_ranges)

rnabase_ranges = [[0, 5, 3], [5, 1201087, 10]]
data = discretization(data, 'RNA Base', rnabase_ranges)

rna4_ranges = [[0, 5, 3], [5, 1201716, 10]]
data = discretization(data, 'RNA 4', rna4_ranges)

rna12_ranges = [[0, 5, 3], [5, 3731528, 10]]
data = discretization(data, 'RNA 12', rna12_ranges)

rnaeot_ranges = [[0, 5, 3], [5, 808451, 10]]
data = discretization(data, 'RNA EOT', rnaeot_ranges)

data = discretization(data, 'RNA EF', rnaeot_ranges)

# data cleaning
data = data.drop('RNA EF', axis=1)
data = data.drop('RNA EOT', axis=1)
data = data.drop('RNA 12', axis=1)
data = data.drop('RNA 4', axis=1)
data = data.drop('RNA Base', axis=1)
data = data.drop('ALT 48', axis=1)
data = data.drop('ALT 36', axis=1)
data = data.drop('ALT 24', axis=1)
data = data.drop('ALT 12', axis=1)
data = data.drop('ALT 1', axis=1)
data = data.drop('AST 1', axis=1)
data = data.drop('Plat', axis=1)
data = data.drop('RBC', axis=1)

sss = StratifiedShuffleSplit(n_splits=5, test_size=0.2, random_state=0)
sss.get_n_splits(data, y_classes)
for train_index, test_index in sss.split(data, y_classes):
    X_train, X_test = data.iloc[train_index], data.iloc[test_index]
    y_train, y_test = y_classes.iloc[train_index], y_classes.iloc[test_index]

#X_train, y_train, X_test, y_test = train_test_splitter(data, y_classes, 0.2)
clf = SVC().fit(X_train, y_train.values.ravel())

predicted = clf.predict(X_test)
accuracy_score(predicted, y_test)