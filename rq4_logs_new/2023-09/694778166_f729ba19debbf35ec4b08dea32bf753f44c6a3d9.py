import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import mean_squared_error, accuracy_score
import numpy as np


def split_data(df, test_size=0.2, random_state=42):
    # Split the dataset into training and testing sets
    return train_test_split(df, test_size=test_size, random_state=random_state)


def separate_features_target(df, target_column):
    # Separate features and target variable
    features = df.drop([target_column], axis=1)
    target = df[target_column].copy()
    return features, target


def train_linear_regression(features, target):
    # Initialize and fit a linear regression model
    lin_reg = LinearRegression()
    lin_reg.fit(features, target)
    return lin_reg


def train_logistic_regression(features, target):
    # Initialize and fit a logistic regression model
    log_reg = LogisticRegression(max_iter=10000, solver='lbfgs')
    log_reg.fit(features, target)
    return log_reg


def calculate_rmse(model, features, target):
    # Make predictions using the model and calculate RMSE
    predictions = model.predict(features)
    mse = mean_squared_error(target, predictions)
    rmse = np.sqrt(mse)
    return rmse


if __name__ == "__main__":
    # Load the dataset
    data_frame = pd.read_csv("winequality-white.csv", sep=";")

    # Display the first 100 rows of the dataset
    print(data_frame.head(100))

    # Split the dataset into training and testing sets
    train_set, test_set = split_data(data_frame)

    # *************** TRAINING SET ***************
    # Separate features and target variable in the training set
    train_wine_features, train_wine_labels = separate_features_target(train_set, target_column="quality")

    # Initialize and train a linear regression model
    lin_reg_model = train_linear_regression(train_wine_features, train_wine_labels)

    # Calculate and display the RMSE for the linear regression model
    train_linear_rmse = calculate_rmse(lin_reg_model, train_wine_features, train_wine_labels)
    print(f"Linear Regression RMSE for TRAIN: {train_linear_rmse}")

    # Initialize and train a logistic regression model for classification
    log_reg_model = train_logistic_regression(train_wine_features, train_wine_labels)

    # Perform classification on the training set
    log_reg_train_predictions = log_reg_model.predict(train_wine_features)

    # Calculate and display the accuracy for the logistic regression model
    log_reg_train_accuracy = accuracy_score(train_wine_labels, log_reg_train_predictions)
    print(f"Logistic Regression Accuracy for TRAIN: {log_reg_train_accuracy}")

    # *************** TESTING SET ***************
    # Separate features and target variable in the training set
    test_wine_features, test_wine_labels = separate_features_target(test_set, target_column="quality")

    # Calculate and display the RMSE for the linear regression model
    test_linear_rmse = calculate_rmse(lin_reg_model, test_wine_features, test_wine_labels)
    print(f"Linear Regression RMSE for TEST: {test_linear_rmse}")

    # Make predictions using the logistic regression model
    log_reg_test_predictions = log_reg_model.predict(test_wine_features)

    # Calculate and display the accuracy for the logistic regression model
    log_reg_test_accuracy = accuracy_score(test_wine_labels, log_reg_test_predictions)
    print(f"Logistic Regression Accuracy for TEST: {log_reg_test_accuracy}")