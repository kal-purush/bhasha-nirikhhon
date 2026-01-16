import pandas as pd
from google.cloud import storage
import numpy as np
import joblib


### AWS Parameters ###
AWS_BUCKET_PATH = "s3://wagon-public-datasets/taxi-fare-train.csv"

### GCP Parameters ### 
BUCKET_NAME = 'wagon-ta-ml-paulo-01'
BUCKET_TRAIN_DATA_PATH = 'data/train_1k.csv'
MODEL_NAME = 'taxifare'
MODEL_VERSION = 'v1'
STORAGE_lOCATION = 'models'
FILENAME = "model.joblib"

def get_data(nrows=10_000):
    '''returns a DataFrame with nrows from s3 bucket'''
    
    ###  Loading Data from AWS S3 ###
    ##df = pd.read_csv(AWS_BUCKET_PATH, nrows=nrows) # Using AWS S3 file
    
    ### Loading Data from our own GCP Storage ###
    client = storage.Client()
    df = pd.read_csv(f"gs://{BUCKET_NAME}/{BUCKET_TRAIN_DATA_PATH}", nrows=1000)
    
    return df


def clean_data(df, test=False):
    df = df.dropna(how='any', axis='rows')
    df = df[(df.dropoff_latitude != 0) | (df.dropoff_longitude != 0)]
    df = df[(df.pickup_latitude != 0) | (df.pickup_longitude != 0)]
    if "fare_amount" in list(df):
        df = df[df.fare_amount.between(0, 4000)]
    df = df[df.passenger_count < 8]
    df = df[df.passenger_count >= 0]
    df = df[df["pickup_latitude"].between(left=40, right=42)]
    df = df[df["pickup_longitude"].between(left=-74.3, right=-72.9)]
    df = df[df["dropoff_latitude"].between(left=40, right=42)]
    df = df[df["dropoff_longitude"].between(left=-74, right=-72.9)]
    return df


def save_model(reg):
    """method that saves the model into a .joblib file and uploads it on Google Storage /models folder
    HINTS : use joblib library and google-cloud-storage"""

    # saving the trained model to disk is mandatory to then beeing able to upload it to storage
    # Implement here
    
    joblib.dump(reg, FILENAME)
    print(f"saved {FILENAME} locally")

    # Implement here
    client = storage.Client()
    # https://console.cloud.google.com/storage/browser/[bucket-id]/
    
    # Get GCP bucket
    bucket = client.get_bucket(BUCKET_NAME)

    # Prepare filename in GCP Storage 
    blob = bucket.blob(f'{STORAGE_lOCATION}/{MODEL_NAME}/{MODEL_VERSION}/{FILENAME}')
    blob.upload_from_filename(filename=FILENAME)

    print(f"uploaded {FILENAME} to gcp cloud storage under \n => {STORAGE_lOCATION}/{MODEL_NAME}/{MODEL_VERSION}/{FILENAME}")

if __name__ == '__main__':
    df = get_data()