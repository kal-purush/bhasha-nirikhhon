# Importing the os module for interacting with the operating system
import os
# Importing the warnings module to manage warning messages
import warnings
# Importing the sys module to interact with the Python runtime environment
import sys

# Importing pandas as pd for data manipulation and analysis
import pandas as pd
# Importing numpy as np for numerical computations
import numpy as np

# Importing specific metrics for evaluating model performance from sklearn
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
# Importing a function to split data into training and testing sets from sklearn
from sklearn.model_selection import train_test_split
# Importing the ElasticNet linear regression model from sklearn
from sklearn.linear_model import ElasticNet
# Importing the urlparse function from urllib.parse to handle URLs
from urllib.parse import urlparse

# Importing the mlflow module for experiment tracking and model management
import mlflow
# Importing a function to infer the input and output signature of a model from mlflow
from mlflow.models.signature import infer_signature
# Importing sklearn utilities from mlflow for logging sklearn models
import mlflow.sklearn

# Importing the logging module to log messages
import logging

# Configuring the logging module to display messages with a specified severity level
logging.basicConfig(level=logging.INFO)
# Creating a logger object
logger = logging.getLogger(__name__)


# Defining a function to evaluate model metrics
def eval_metrics(actual, pred):
    # Calculating root mean squared error
    rmse = np.sqrt(mean_squared_error(actual, pred))
    # Calculating mean absolute error
    mae = mean_absolute_error(actual, pred)
    # Calculating R-squared score
    r2 = r2_score(actual, pred)
    # Returning the calculated metrics
    return rmse, mae, r2


if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    np.random.seed(40)

# Specifying the URL of the CSV file to be used as the dataset

url = 'D:\mlflow\wine_quality\winequality-red.csv'
# csv_url = ( 
#     "https://github.com/YonSci/ml_flow_experment/blob/main/dataset/winequality-red.csv"
# )


data = None  # Initialize `data` to ensure it's defined

# Attempting to read the CSV file from the specified URL
try:
    data = pd.read_csv(url, sep=',')
# Handling exceptions that occur during the read operation
except Exception as e:
    # Logging an error message if the CSV file cannot be read
    logger.exception(
        "Unable to download training & test CSV, check your internet connection. Error: %s", e
    )
    exit()

# Splitting the data into training and test sets
if data is not None:
    
    train, test = train_test_split(data)

else:
    logger.error("No data to train on.")


# Dropping the target variable 'quality' from the training dataset
train_x = train.drop(["quality"], axis=1)
# Dropping the target variable 'quality' from the test dataset
test_x = test.drop(["quality"], axis=1)

# Extracting the target variable 'quality' from the training dataset
train_y = train[['quality']]
# Extracting the target variable 'quality' from the test dataset
test_y = test[['quality']]

# Setting the alpha parameter for the ElasticNet model, defaulting to 0.5 if not provided
alpha = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5
# Setting the l1_ratio parameter for the ElasticNet model, defaulting to 0.5 if not provided
l1_ratio = float(sys.argv[2]) if len(sys.argv) > 2 else 0.5

# Starting an MLflow run to track the model training process
with mlflow.start_run():
    # Creating an instance of the ElasticNet model with specified alpha and l1_ratio
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
    # Training the ElasticNet model with the training dataset
    lr.fit(train_x, train_y)

    # Predicting the target variable for the test dataset
    pred_y = lr.predict(test_x)

    # Evaluating the model's performance using the test dataset
    (rmse, mae, r2) = eval_metrics(test_y, pred_y)

    # Printing the model parameters and evaluation metrics
    print("Elasticnet Model (alpha={:f}, l1_ratio={:f}):" .format(alpha, l1_ratio))
    print("  RMSE: %s" % rmse)
    print("  MAE: %s" % mae)
    print("  R2: %s" % r2)

    # Logging the model parameters to MLflow
    mlflow.log_param("alpha", alpha)
    mlflow.log_param("l1_ratio", l1_ratio)
    # Logging the evaluation metrics to MLflow
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    mlflow.log_metric("mae", mae)

    remote_server_url = "https://dagshub.com/YonSci/ml_flow_experment.mlflow"
    mlflow.set_tracking_uri(remote_server_url)

    # # Predicting the target variable for the training dataset to infer the signature
    # predictions = lr.predict(train_x)
    # # Inferring the input and output signature of the trained model
    # signature = infer_signature(train_x, predictions)

    # Parsing the tracking URI to determine the type of store being used
    tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme

    # Checking if the tracking store  is not a local file store
    if tracking_url_type_store != "file":
        # Logging the trained model to MLflow with a specified model name
        mlflow.sklearn.log_model(
            lr, "model", registered_model_name="ElasticnetWineModel"
        )
    # If the tracking store is a local file store
    else:
        # Logging the trained model to MLflow
        mlflow.sklearn.log_model(lr, "model")