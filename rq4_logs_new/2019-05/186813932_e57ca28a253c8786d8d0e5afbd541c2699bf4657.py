#!/usr/bin/env python3
#
# This code traverses each region in AWS and searches for resources in EC2
# that match tags provided.
#
# Usage: `basename $0` [-v][-r region1,region2] [-p profile] 
#                      <tag1=value,tag2=value,...,tagN=value>
#
# Requires: Python3, Boto 3
#
# Before you can begin using Boto 3, you should set up authentication 
# credentials. Credentials for your AWS account can be found in the IAM
# Console. You can create or use an existing user. Go to manage access keys and
# generate a new set of keys.
#
# If you have the AWS CLI installed, then you can use it to configure your credentials file:
# `aws configure`
#
# Author: Justin Cook <jhcook@secnix.com>

import sys, os, json, argparse, zlib
from datetime import datetime

try:
  import boto3
  from diskcache import Cache, Disk
except ModuleNotFoundError as err:
  print(err, file=sys.stderr)
  sys.exit(1)

class JSONDiskCache(Disk):
  def __init__(self, directory, compress_level=1, **kwargs):
    self.compress_level = compress_level
    super(JSONDiskCache, self).__init__(directory, **kwargs)

  def put(self, key):
    json_bytes = json.dumps(key).encode('utf-8')
    data = zlib.compress(json_bytes, self.compress_level)
    return super(JSONDiskCache, self).put(data)

  def get(self, key, raw):
    data = super(JSONDiskCache, self).get(key, raw)
    return json.loads(zlib.decompress(data).decode('utf-8'))

  def store(self, value, read):
    if not read:
      json_bytes = json.dumps(value).encode('utf-8')
      value = zlib.compress(json_bytes, self.compress_level)
    return super(JSONDiskCache, self).store(value, read)

  def fetch(self, mode, filename, value, read):
    data = super(JSONDiskCache, self).fetch(mode, filename, value, read)
    if not read:
      data = json.loads(zlib.decompress(data).decode('utf-8'))
    return data

class DateTimeEncoder(json.JSONEncoder):
  '''some objects are not serialisable to json'''
  def default(self, o):
    if isinstance(o, datetime):
      return o.isoformat()
    elif isinstance(o, boto3.Session) or isinstance(o, boto3.ec2):
      return ''
    return json.JSONEncoder.default(self, o)

class EC2Resources:
  def __init__(self, session, filters, region=None):
    self.__session = session
    self.region = region
    self.__conn = self.__session.client("ec2", region_name=self.region)
    self.filters = filters
    self.instances = self.__conn.describe_instances
    self.volumes = self.__conn.describe_volumes

  @property
  def filters(self):
    return self.__filters
  
  @filters.setter
  def filters(self, filters):
    self.__filters = filters

  @property
  def region(self):
    return self.__region
  
  @region.setter
  def region(self, region):
    self.__region = region

  @property
  def instances(self):
    '''Get the instances'''
    return self.__instances

  @instances.setter
  def instances(self, callableFunc):
    self.__instances = callableFunc(Filters=self.filters)

  @property
  def volumes(self):
    '''Get the volumes'''
    return self.__volumes
  
  @volumes.setter
  def volumes(self, callableFunc):
    self.__volumes = callableFunc(Filters=self.filters)

def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument("-v", "--verbosity", action="count", default=0,
                      help="print debugging messages. Multiple -v options " +
                      "increase verbosity.  The maximum is 3.")
  parser.add_argument("-r", "--region", type=str, 
                      help="use specified region(s) '-r us-west-1,us-west-2' " +
                      "defaults to configured region in profile. Use '-r all' " +
                      "for all regions.")
  parser.add_argument("-p", "--profile", type=str, 
                      help="use specified profile") 
  parser.add_argument("tags", type=str, help="specify tags to search, e.g. " +
                      "stack_name=test,test01;role=hadoop,hbase")
  args = parser.parse_args()
  args.region = args.region.split(',')
  args.tags = [dict(item.split("=") for item in args.tags.split(";"))]
  return args

def main():
  # Sort command-line parameters
  args = parse_args()

  filters = [{'Name':'tag:{}'.format(list(item.keys())[0]), 
              'Values':list(item.values())} for item in args.tags]
  if args.verbosity: print("filters: {}".format(filters))

  # Create a session and connection to ec2
  session = boto3.Session(profile_name=args.profile)
  conn = session.client('ec2')

  # Create region list
  if args.region == 'all':
    userRegion = [ region['RegionName'] for region in [ region for region in 
    conn.describe_regions()['Regions']]]
  else:
    userRegion = args.region
  if args.verbosity: print("userRegion: {}".format(userRegion))

  # Cache results to disk
  cache = Cache(os.path.expanduser('~') + '/.awstools', disk=JSONDiskCache,
                disk_compress_level=6)
  
  regions = []
  for region in userRegion:
    try:
      regions.append(cache[region + str(filters)])
    except KeyError:
      regions.append({region:EC2Resources(session, filters, region)})

  #regions = [ {region:EC2Resources(session, filters, region)} for region in 
  #            userRegion ]
  if args.verbosity>1: print("regions: {}".format(regions))

  for region in regions:
    for k, ec2Instance in region.items():
      # Convert to JSON -> Python data structure -> JSON for proper formatting
      jsonContent = json.dumps(ec2Instance.instances, cls=DateTimeEncoder)
      from_json = json.loads(jsonContent) # Shrugs
      print(json.dumps(from_json, indent=4))
      #cache.add(k + str(ec2Instance.filters), json.dumps(ec2Instance.__dict__,
      #                                                   cls=DateTimeEncoder))

if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    print("Exiting on user interrupt")
    sys.exit(0)