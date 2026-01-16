import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, roc_auc_score
from send_request import prediction_request

def prepare_data(test_data_dir):
    # read test dataset
    test = pd.read_csv(test_data_dir)
    X_test = np.array((test.iloc[:, :-1])).reshape(len(test),2).tolist()
    y_test = np.array(test.iloc[:, -1:]).reshape(len(test)).tolist()
    return X_test, y_test
    
    
if __name__=="__main__":
    test_data = 'evaluation/datasets/test.csv'
    x_test, y_test = prepare_data(test_data)
    test_pred = prediction_request(x_test)
    print("f1 score: ")
    print(f1_score(y_test, test_pred))
    print("ROC AUC score: ")
    print(roc_auc_score(y_test, test_pred))