import pandas as pd


df = pd.read_csv('data.csv', header=None)
df.columns = df.loc[0, :]
df = df.iloc[1:, :]

X = df.loc[:, ('height', 'midtermGrade', 'hoursOfStudy')]
y = df.loc[:, 'label']


def split_data(X, y, test_size=0.3):

    X_shuffled = X.sample(frac=1)
    y_shuffled = y.sample(frac=1)
    length = X.count()[1]
    i = int((1.0 - test_size ) * length)
    X_train = X_shuffled.iloc[:i, :]
    X_test = X_shuffled.iloc[i:, :]
    y_train = y_shuffled.iloc[:i]
    y_test = y_shuffled.iloc[i:]

    return X_train, X_test, y_train, y_test


X_train, X_test, y_train, y_test = split_data(X, y)

