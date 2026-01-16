import pickle
from lightgbm import LGBMClassifier
import pandas as pd
from Pydantic import BaseModel


def predict(customer_id):
    df = pd.read_csv('X_sample.csv', index_col="SK_ID_CURR")
    model = pickle.load(open("LGBMClassifier.pkl", "rb"))

    customer_df = df[df.index == customer_id]

    if customer_df.shape[0] == 0:
        return -1
    X = customer_df.iloc[:, :-1]
    score = model.predict_proba(X)[0 , 1]
    return {customer_id : round(score*100, 2)}