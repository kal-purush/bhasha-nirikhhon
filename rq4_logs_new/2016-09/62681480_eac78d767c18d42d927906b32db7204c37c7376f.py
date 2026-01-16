from __future__ import print_function, unicode_literals

import pandas as pd
from sklearn.cross_validation import train_test_split
from sklearn.ensemble import RandomForestClassifier

from params import *
import MySQLdb

__author__ = 'David'


def read_table(sql):
    conn = MySQLdb.connect(user=USER, passwd=PASSWORD, db=DB,
                           host=HOST, port=PORT)
    conn.cursor().execute('SET NAMES %s;' % CHARSET)
    df = pd.read_sql_query(sql, conn)
    conn.close()
    print(df.shape, 'data has been loaded.')
    return df


def select_feature(df, fea_start, fea_end, category):
    features = df.columns[fea_start: fea_end]
    label = df.columns[category]
    identify = df.columns[0: 3]
    X = df[features]
    y = df[label]
    ids = df[identify]
    print(fea_start, fea_end, X.shape)
    return X, y, ids


def rf(X, y):
    clf = RandomForestClassifier(n_estimators=10, criterion='gini',
                                 max_depth=None, min_samples_split=2,
                                 min_samples_leaf=1,
                                 min_weight_fraction_leaf=0.0,
                                 max_features='auto', max_leaf_nodes=None,
                                 bootstrap=True, oob_score=False, n_jobs=1,
                                 random_state=None, verbose=0,
                                 warm_start=False, class_weight=None)
    print('RandomForest Classifier is fitting...')
    clf.fit(X, y)
    return clf


def tests():
    def train(sql):
        X, y, _ = select_feature(read_table(
            sql), FEATURE_START, FEATURE_END, LABEL)
        return rf(X, y)

    def test(clfer, sql):
        X_test, y_test, ids = select_feature(read_table(
            sql), FEATURE_START, FEATURE_END, LABEL)

        preds = clfer.predict_proba(X_test)
        test_result = []
        for i, p in enumerate(preds):
            class_prob = zip(clf.classes_, p)
            predictions = insert_sort(class_prob)[:5]
            fu = (list(ids['call_id'])[i], list(
                ids['cust_id'])[i], list(ids['call_time'])[i])
            test_result.append(
                (fu, predictions, list(y_test)[i]))
            if not i % 1000:
                print(i)

        mat = get_matrix(test_result, list(clf.classes_))
        sql = 'INSERT INTO ivr_rec VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        conn = MySQLdb.connect(user=USER, passwd=PASSWORD, db=DB,
                               host=HOST, port=PORT)
        cursor = conn.cursor()
        cursor.execute('SET NAMES %s;' % CHARSET)
        cursor.executemany(sql, mat)
        conn.commit()
        print('Done.')
        cursor.close()
        conn.close()

    def insert_sort(lists):
        count = len(lists)
        for i in range(1, count):
            key = lists[i]
            j = i - 1
            while j >= 0:
                if lists[j][1] < key[1]:
                    lists[j + 1] = lists[j]
                    lists[j] = key
                j -= 1
        return lists

    def get_matrix(result, classes):
        mat = []
        tmp = ['', '', '', '']
        for r in result:
            l = [rr for rr in r[0]]
            l.append(r[-1])
            if tmp == l:
                continue
            else:
                tmp = l
            for pred in r[1]:
                l.append(pred[0])
            mat.append(l)
        return mat

    for i in range(0, 120000, 10000):
        sql = DB_SQL % i
        if i == 0:
            clf = train(sql)
        else:
            test(clf, sql)

if __name__ == '__main__':
    tests()