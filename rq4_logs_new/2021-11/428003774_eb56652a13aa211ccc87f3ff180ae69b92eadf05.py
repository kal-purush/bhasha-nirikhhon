import pandas as pd
import numpy as np
import pickle
from preprocess import process_ds
from sklearn.preprocessing import LabelEncoder


def transform_cat_feats(df):
    """makes null columns into unknown and cat columns
    are label encoded
    Args:
    df (pd.DataFrame): Dataframe with the sales data.
    Returns:
    Dataframe with the sales data including lag and rolling
    features.
    """
    # nan_features = [
    #'event_name_1',
    #'event_type_1',
    #'event_name_2',
    #'event_type_2',]

    # for feature in nan_features:
    #    df[feature].fillna('unknown', inplace = True)

    cat = [
        "item_id",
        "dept_id",
        "cat_id",
        "store_id",
        "state_id",
        "event_name_1",
        "event_type_1",
        "event_name_2",
        "event_type_2",
    ]

    for feature in cat:
        encoder = LabelEncoder()
        df[feature] = encoder.fit_transform(df[feature])

    return df


def calculate_time_features(df):
    """Clagged and rolling mean features
    of the sales data.
    Args:
    df (pd.DataFrame): Dataframe with the sales data.
    Returns:
    Dataframe with the sales data including lag and rolling
    features.
    """

    dayLags = [28]
    lagSalesCols = [f"lag_{dayLag}" for dayLag in dayLags]
    for dayLag, lagSalesCol in zip(dayLags, lagSalesCols):
        df[lagSalesCol] = (
            df[["id", "item_sales"]].groupby("id")["item_sales"].shift(dayLag)
        )

    windows = [7, 28]
    for window in windows:
        for dayLag, lagSalesCol in zip(dayLags, lagSalesCols):
            df[f"rmean_{dayLag}_{window}"] = (
                df[["id", lagSalesCol]]
                .groupby("id")[lagSalesCol]
                .transform(lambda x: x.rolling(window).mean())
            )

    return df


def cat_ts_feats(df):
    """Build categorical and time series feats.
    Args:
    df (pd.Dataframe) : Dataframe with sales data
    Returns:
    Dataframe with sales data including categorical
    features and lag/rolling mean features
    """
    df = transform_cat_feats(df)
    df = calculate_time_features(df)

    return df


def get_test_train_data():
    """Build train and test dataset. Test is
    used for inference
    Args:
    None
    Returns:
    train and test dataframes
    """
    df = process_ds()
    df = cat_ts_feats(df)
    df = df.reset_index().set_index("date")
    # remove unused columns
    cols_not_used = ["id", "weekday", "d", "index"]
    df.drop(columns=cols_not_used, inplace=True)
    df.dropna(inplace=True)
    # convert T/F to boolean - lightgbm throws error otherwise
    df["is_weekend"] = df["is_weekend"].astype(int)
    df["no_sell_price"] = df["no_sell_price"].astype(int)

    print(df)

    train_start_date = "2014-04-24"
    train_end_date = "2016-04-23"
    test_start_date = "2016-04-24"
    test_end_date = "2016-05-23"

    df_train = df.loc[train_start_date:train_end_date]
    df_test = df.loc[test_start_date:test_end_date]

    # save train and test dataframes for later use
    df_train.to_pickle("../data/df_train.pkl")
    df_test.to_pickle("../data/df_test.pkl")


if __name__ == "__main__":
    get_test_train_data()