import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Configuration
AWS_ACCESS_KEY_ID = "admin"
AWS_SECRET_ACCESS_KEY = "admin123"
ENDPOINT_URL = "http://127.0.0.1:9000"  # Example: "http://localhost:4566" for localstack
BUCKET_NAME = "my-bucket"
FOLDER_PATH = "test_dir"

# Initialize S3 Client
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=ENDPOINT_URL,
)

def create_bucket(bucket_name):
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")

        # Enable versioning
        s3.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"}
        )
        print(f"Versioning enabled for bucket '{bucket_name}'.")
    except s3.exceptions.BucketAlreadyExists as e:
        print(f"Bucket already exists: {e}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket '{bucket_name}' already owned by you.")

def upload_folder(bucket_name, folder_path):
    try:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                key = os.path.relpath(file_path, start=folder_path).replace("\\", "/")
                s3.upload_file(file_path, bucket_name, key)
                print(f"Uploaded {file_path} as {key}")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except NoCredentialsError:
        print("AWS credentials not found.")
    except PartialCredentialsError:
        print("Incomplete AWS credentials provided.")

def list_bucket_contents(bucket_name):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            print(f"\nContents of bucket '{bucket_name}':")
            for obj in response["Contents"]:
                print(f" - {obj['Key']} (Size: {obj['Size']} bytes)")
        else:
            print(f"\nBucket '{bucket_name}' is empty.")
    except s3.exceptions.NoSuchBucket:
        print(f"Bucket '{bucket_name}' does not exist.")
    except NoCredentialsError:
        print("AWS credentials not found.")
    except PartialCredentialsError:
        print("Incomplete AWS credentials provided.")


def list_bucket_contents_with_versions(bucket_name):
    try:
        response = s3.list_object_versions(Bucket=bucket_name)
        if "Versions" in response:
            print(f"\nContents of bucket '{bucket_name}' (with versions):")
            for version in response["Versions"]:
                print(
                    f" - {version['Key']} (Version ID: {version['VersionId']}, Size: {version['Size']} bytes, "
                    f"IsLatest: {version['IsLatest']})"
                )
        else:
            print(f"\nBucket '{bucket_name}' has no versions.")
    except s3.exceptions.NoSuchBucket:
        print(f"Bucket '{bucket_name}' does not exist.")
    except NoCredentialsError:
        print("AWS credentials not found.")
    except PartialCredentialsError:
        print("Incomplete AWS credentials provided.")        

# Run the functions
if __name__ == "__main__":
    create_bucket(BUCKET_NAME)
    upload_folder(BUCKET_NAME, FOLDER_PATH)
    list_bucket_contents(BUCKET_NAME)
    list_bucket_contents_with_versions(BUCKET_NAME)