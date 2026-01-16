import boto3
# Creating an S3 access object
obj = boto3.client("s3")
obj.upload_file(
    Filename="C:/Users/admin/Desktop/gfg_logo.png",
    Bucket="mygfgbucket",
    Key="firstgfgbucket.png"
)
# Downloading S3 file to local
obj.download_file(
    Filename="Desktop/DownloadedFile.csv",
    Bucket="mygfgbucket",
    Key="SampleSpreadsheet.csv"
)