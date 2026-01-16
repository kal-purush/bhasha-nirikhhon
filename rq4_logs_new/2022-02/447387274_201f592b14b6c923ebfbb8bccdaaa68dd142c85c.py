import cssdata
import json
import numpy as np

input = json.load(open("input.json", "r"))


def select_datasets(dataset_ids):
    dfs = list()
    for dataset_id in dataset_ids:
        df = cssdata.csv_to_dataframe(input=input, exp_id=dataset_id["exp_id"], trial_id=dataset_id["trial_id"])
        dfs.append(df)
    return dfs


def normalize(dfs):
    """
    normalize time by the final time, and normalize shear stress by confining pressure of the test
    """
    normalized_dfs = list()
    for df in dfs:
        confining_pressure = df["Effective Vertical Stress [kPa]"].iloc[0]
        final_time = df["Time [sec]"].iloc[-1]
        normalized_df = df.copy()
        normalized_df["Shear Stress [kPa]"] = df["Shear Stress [kPa]"] / confining_pressure
        normalized_df["Time [sec]"] = df["Time [sec]"] / final_time
        normalized_dfs.append(normalized_df)
    return normalized_dfs


def rnn_inputs(dfs, features, targets, window_length):
    """
    make features and targets datasets, and sample inputs for rnn layers based on time window.
    In addition, return some useful variables.
    :param dfs: dataframes
    :param features: a list of column headers that is used for features
    :param targets: a list of column headers that is used for targets
    :param window_length: the length of the sampling window
    """
    feature_datasets = list()
    target_datasets = list()
    datapoints = list()
    x_rnns = list()
    y_rnns = list()

    for df in dfs:

        # choose columns for features and targets from dfs, and convert them to arrays
        feature_dataset = df[features].to_numpy()
        feature_datasets.append(feature_dataset)
        target_dataset = df[targets].to_numpy()
        target_datasets.append(target_dataset)

        # get the number of datapoints for each df
        datapoint = len(df)
        datapoints.append(datapoint)

        # sample inputs for RNN layers based on time window sampling
        sampled_features = list()
        sampled_targets = list()
        for i in range(datapoint-window_length):
            sampled_feature = feature_dataset[i:i+window_length, :]
            sampled_target = target_dataset[i:i+window_length, :]
            sampled_features.append(sampled_feature)  # shape=(samples, window_length, features)
            sampled_targets.append(sampled_target)  # shape=(samples, window_length, targets)
        x_rnn = np.asarray(sampled_features)
        y_rnn = np.asarray(sampled_targets)
        x_rnns.append(x_rnn)  # shape=(dfs, samples, window_length, features)
        y_rnns.append(y_rnn)  # shape=(dfs, samples, window_length, features)

    rnn_variables = {
        "feature_datasets": feature_datasets,  # feature datasets before sampling
        "target_datasets": target_datasets,  # target datasets before sampling
        "datapoints": datapoints,  # number of datapoint for each dataset
        "x_rnns": x_rnns,  # variable for rnn layer's x input
        "y_rnns": y_rnns  # variable for rnn layer's y input
    }

    return rnn_variables