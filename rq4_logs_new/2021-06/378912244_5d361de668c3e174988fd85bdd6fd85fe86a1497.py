import os
import argparse
from concurrent import futures
import os
import re
import sys

import boto3
import botocore
import tqdm



BUCKET_NAME = 'open-images-dataset'
REGEX = r'(test|train|validation|challenge2018)/([a-fA-F0-9]*)'


def get_images(filepath):

    FILES = os.listdir(filepath)

    return FILES


def check_and_homogenize_one_image(image):
  split, image_id = re.match(REGEX, image).groups()
  yield split, image_id


def check_and_homogenize_image_list(image_list):
  for line_number, image in enumerate(image_list):
    try:
      yield from check_and_homogenize_one_image(image)
    except (ValueError, AttributeError):
      raise ValueError(
          f'ERROR in line {line_number} of the image list. The following image '
          f'string is not recognized: "{image}".')


def read_image_list_file(image_list_file):
  with open(image_list_file, 'r') as f:
    for line in f:
      yield line.strip().replace('.jpg', '')


def download_one_image(bucket, split, image_id, download_folder):
  try:
    bucket.download_file(f'{split}/{image_id}.jpg',
                         os.path.join(download_folder, f'{image_id}.jpg'))
  except botocore.exceptions.ClientError as exception:
    sys.exit(
        f'ERROR when downloading image `{split}/{image_id}`: {str(exception)}')


def download_all_images(args):
  """Downloads all images specified in the input file."""
  bucket = boto3.resource(
      's3', config=botocore.config.Config(
          signature_version=botocore.UNSIGNED)).Bucket(BUCKET_NAME)

  download_folder = args['download_folder'] or os.getcwd()

  if not os.path.exists(download_folder):
    os.makedirs(download_folder)

  try:
    image_list = list(
        check_and_homogenize_image_list(
            read_image_list_file(args['image_list'])))
  except ValueError as exception:
    sys.exit(exception)

  progress_bar = tqdm.tqdm(
      total=len(image_list), desc='Downloading images', leave=True)
  with futures.ThreadPoolExecutor(
      max_workers=args['num_processes']) as executor:
    all_futures = [
        executor.submit(download_one_image, bucket, split, image_id,
                        download_folder) for (split, image_id) in image_list
    ]
    for future in futures.as_completed(all_futures):
      future.result()
      progress_bar.update(1)
  progress_bar.close()

'''
if __name__ == '__main__':
    
    type_split = "validation"

    filepath = "./OID/Dataset/" + type_split + "/Ball"
    FILES = get_images(filepath)
    imagelistFname = './image_list_file.txt'
    

    tmp = []
    with open(imagelistFname, 'w') as f:
        for image_name in FILES:
            if image_name != "Label":
                tmp.append(type_split+ "/" + image_name)

        FILES = tmp
        f.write('\n'.join(FILES))
    

    args = {'image_list': imagelistFname, 'num_processes': 20, 'download_folder': 'OID_ballPerson_' + type_split}

    download_all_images(args)
    '''

from sys import exit
from textwrap import dedent
from modules.parser import *
from modules.utils import *
from modules.downloader import *
from modules.show import *
from modules.csv_downloader import *
from modules.bounding_boxes import *
from modules.image_level import *
ROOT_DIR = ''
DEFAULT_OID_DIR = os.path.join(ROOT_DIR, 'OID')

if __name__ == '__main__':

    args = parser_arguments()

    

    if args.command == 'downloader_ill':
        image_level(args, DEFAULT_OID_DIR)
    else:
        bounding_boxes_images(args, DEFAULT_OID_DIR)

