import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis


def read(path='../../Animals_with_Attributes2/Features/ResNet101'):
    X = pd.read_csv(path + '/AwA2-features.txt', sep=' ', names=[f'feature{i}' for i in range(2048)])
    y = pd.read_csv(path + '/AwA2-labels.txt', names=['label'])
    return X, y


def load_data(path='data', reduce=False):
    X_train = np.load(path + 'lda/X_train.npy') if reduce else np.load(path + '/X_train.npy')
    X_test = np.load(path + 'lda/X_test.npy') if reduce else np.load(path + '/X_test.npy')
    y_train = np.squeeze(np.load(path + '/y_train.npy'))
    y_test = np.squeeze(np.load(path + '/y_test.npy'))
    return X_train, X_test, y_train, y_test


def reduce_dim_and_divide_data(path='data/lda', test=0.4, reduce=True):
    X, y = read()
    y = np.squeeze(y)
    if reduce:
        lda = LinearDiscriminantAnalysis()  # reduce to (n_classes - 1)-dim
        lda.fit(X, y)
        X = lda.transform(X)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test, random_state=422)
    np.save(path + '/X_train', X_train)
    np.save(path + '/X_test', X_test)
    np.save(path + '/y_train', y_train)
    np.save(path + '/y_test', y_test)


if __name__ == '__main__':
    reduce_dim_and_divide_data(path='data', reduce=False)