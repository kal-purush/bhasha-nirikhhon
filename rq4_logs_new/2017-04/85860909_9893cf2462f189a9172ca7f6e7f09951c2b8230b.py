#!/usr/bin/env python

import sys, os
from pcfgcp import PcfGcp
from google.cloud import storage

# Upload a file, list it, delete it?
if len(sys.argv) != 2:
  print 'Usage: %s file_name' % sys.argv[0]
  sys.exit(1)

srcFileName = sys.argv[1]
print 'File: %s' % srcFileName

pg = PcfGcp()
storage_client = pg.getStorage()
bucketName = pg.getBucketName()
print 'Bucket: %s' % bucketName

def list_blobs(bucket_name):
  bucket = storage_client.get_bucket(bucket_name)
  blobs = bucket.list_blobs()
  for blob in blobs:
    print(blob.name)

"""Uploads a file to the bucket."""
def upload_blob(bucket_name, source_file_name, destination_blob_name):
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)
  blob.upload_from_filename(source_file_name)
  print('File {} uploaded to {}.'.format(
    source_file_name,
    destination_blob_name))

"""Downloads a blob from the bucket."""
def download_blob(bucket_name, source_blob_name, destination_file_name):
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(source_blob_name)
  blob.download_to_filename(destination_file_name)
  print('Blob {} downloaded to {}.'.format(
    source_blob_name,
    destination_file_name))

list_blobs(bucketName)
upload_blob(bucketName, srcFileName, srcFileName)
list_blobs(bucketName)
download_blob(bucketName, srcFileName, srcFileName + '.NEW')

