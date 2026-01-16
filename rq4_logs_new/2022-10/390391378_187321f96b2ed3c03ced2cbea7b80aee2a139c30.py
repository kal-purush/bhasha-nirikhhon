# CatBoost Regressor Example
#If you don't have catboost You can install it from below line
#!pip install catboost
from sklearn.datasets import load_diabetes
from catboost import CatBoostRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split


def main():

    """
    The Url for the algorithm
    https://catboost.ai/en/docs/concepts/python-reference_catboostregressor
    Diabetes  dataset is used to demonstrate the algorithm.
    For more detailed example got to
    https://www.kaggle.com/code/modassirafzal/housing-top-3
    """
    # Load Diabetes dataset
    diabetes = load_diabetes()
    print(diabetes.keys())

    # Split dataset into train and test data
    x = diabetes["data"]  # features
    y = diabetes["target"]
    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.25
    )

    #CatBoost Regressor
    cbr=CatBoostRegressor(num_trees=50)
    cbr.fit(x_train, y_train)

    # Predict target for test data
    predictions = cbr.predict(x_test)
    predictions = predictions.reshape(len(predictions), 1)

    # Error printing
    print(f"Mean Absolute Error:\t {mean_absolute_error(y_test, predictions)}")
    print(f"Mean Square Error  :\t {mean_squared_error(y_test, predictions)}")
    print(cbr.plot_tree(tree_idx=0))

if __name__ == "__main__":
    main()