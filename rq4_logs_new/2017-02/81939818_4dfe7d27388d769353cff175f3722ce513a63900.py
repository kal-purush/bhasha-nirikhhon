"""This module generates random candidates data and
sends packed gzip in JSON format to AWS S3 bucket """
import logging
import random
import gzip
import json
import shutil
import datetime
import ConfigParser
from time import time, sleep
import boto3
import botocore

FAKE_CANDIDATES = ['John Smiths', 'Nicolas Robins', 'Andrew Robinson', 'James Floyd',
                   'Steven Harris', 'Wendy Smart', 'Joe Hampton', 'Ralph Robertson',
                   'Alex Stevenson', 'Ted Kowalsky', 'Fred Novak', 'John Dummy']


S3_CLIENT = boto3.resource('s3')   #creating s3 client

# pylint: disable=C0103
config_parser = ConfigParser.RawConfigParser()
config_parser.read('candidates_feed.cfg')
PACKAGE_SIZE = config_parser.getint('General', 'package-size')
FILENAME_PATTERN = config_parser.get('General', 'filename-pattern')
TIMESTAMP_FORMAT = config_parser.get('General', 'timestamp-format')
ANOMALIES_FREQUENCY = config_parser.getint('General', 'anomalies-frequency')
UPLOAD_INTERVAL = config_parser.getint('General', 'upload-interval')
BUCKET_NAME = config_parser.get('AWS', 'bucket-name')


def upload_file_to_s3(file_name, bucket_name):
    """
    Uploads file to amazon S3 bucket

    Args:
        file_name (str): name of a file to upload
        bucket_name (str): name of a target S3 bucket where file will be stored
    """
    logging.info("uploading file to s3")
    with open(file_name, 'rb') as file_data:
        try:
            S3_CLIENT.Bucket(bucket_name).put_object(Key=file_name, Body=file_data)
        except botocore.exceptions.ClientError as e:
            logging.error("Unable to put object to S3: %s. Error occurred: %s", bucket_name, e)


def get_candidate(accuracy, name):
    """
    Returns candidate entry with given accuracy and name

    Args:
        accuracy (int): Candidate's accuracy value
        name (str): Candidate's full name

    Returns:
        dict: Dictionary containing candidate's name and accuracy
    """
    candidate = {}
    candidate['accuracy'] = accuracy
    candidate['name'] = name
    return candidate


def get_normal_candidate():
    """
    Returns candidate entry with lower accuracy values

    Returns:
        dict: Dictionary containing candidate's name and accuracy

    """
    candidate_accuracy = random.randint(10, 20)
    name = random.choice(FAKE_CANDIDATES)
    return get_candidate(candidate_accuracy, name)


def get_better_candidate():
    """
    Returns candidate entry with high accuracy values

    Returns:
        dict: Dictionary containing candidate's name and accuracy
    """
    candidate_accuracy = random.randint(90, 100)
    name = 'Wojciech Czyz'
    return get_candidate(candidate_accuracy, name)


def dump_candidates_to_gzipped_json(candidates):
    """
    Creates a compressed .gzip file from JSON data

    Args:
        candidates (list): list of candidates dictionary entries

    Returns:
        str: generated name of gzip file where the list of candidates is stored
    """
    stamp = datetime.datetime.fromtimestamp(time()).strftime(TIMESTAMP_FORMAT)
    json_file_name = 'temp_file.json'
    gzip_file_name = FILENAME_PATTERN.format(stamp)
    with open(json_file_name, 'w') as json_file:
        json.dump(candidates, json_file)
    with open(json_file_name, 'rb') as f_in, gzip.open(gzip_file_name.format(stamp), 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    return gzip_file_name


def main():
    """
    Main function that generates the feed of candidates and uploads aggregated file to s3
    """
    candidates_list = []
    while True:     #infinite loop that executes main script functions till keyboard interrupt
        rnd = random.random()
        if rnd < ANOMALIES_FREQUENCY/100:
            logging.debug("Generating candidate with better accuracy")
            candidates_list.append(get_better_candidate())
        else:
            candidates_list.append(get_normal_candidate())

        if len(candidates_list) == PACKAGE_SIZE:
            logging.debug("Package size reached - creating and uploading file...")
            gzipped_json_filename = dump_candidates_to_gzipped_json(candidates_list)
            upload_file_to_s3(gzipped_json_filename, BUCKET_NAME)
            logging.debug("candidates list: %s", candidates_list)
            print "Candidates list: %s" % (candidates_list)
            sleep(UPLOAD_INTERVAL)
            candidates_list = []

if __name__ == '__main__':
    main()