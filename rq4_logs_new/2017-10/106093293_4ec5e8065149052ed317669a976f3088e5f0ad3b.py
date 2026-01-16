
# coding: utf-8

# In[20]:

#import relevent packages
import urllib3
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import os
import shutil
import logging
import argparse
import boto3
from botocore.client import Config


# In[21]:

# create logger with 'CaseStudy1Part1'
logger = logging.getLogger('CaseStudy1Part1')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('CaseStudy1Part1.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


# In[22]:

parser = argparse.ArgumentParser('CaseStudy1Part1')
parser.add_argument('-c','--cik', help='The CIK of the company for which the filling is to be downloaded.', type=str, default = '0000051143', nargs = '?')
parser.add_argument('-a','--accno', help='The accession number of the company for which the filling is to be downloaded.', type=str, default = '0000051143-13-000007', nargs = '?')
parser.add_argument('-f','--ftf', help='The type of the file to be downloaded.', type=str, default = '10-Q', nargs = '?')
parser.add_argument('-k','--awskey', help='The key to the aws account where the files is to be uploaded.', type=str)
parser.add_argument('-s','--awssecretkey', help='The secret key to the aws account where the files is to be uploaded', type=str)
parser.add_argument('-b','--awsbucket', help='The bucket where the files is to be uploaded', type=str)
args = parser.parse_args()


# In[23]:

cik = args.cik
accno = args.accno
file_tf = args.ftf
awskey = args.awskey
awssecretkey = args.awssecretkey
awsbucket = args.awsbucket


# In[24]:

cik_nz = cik.lstrip('0')
accno_nh = accno.replace('-','')


# In[25]:

url = 'http://www.sec.gov/Archives/edgar/data/' + cik_nz + '/' + accno_nh + '/' + accno+'-index.html'


# In[26]:

http = urllib3.PoolManager()
response = http.request('GET',url)


# In[27]:

soup = BeautifulSoup(response.data,'lxml')
table = soup.find('table',{'summary':'Document Format Files'})
rows = table.findAll('tr')
for row in rows:
    cols = row.findAll('td')
    for col in cols:
        if col.contents[0] == file_tf:
            extension = row.find('a',href = True).contents
            break               


# In[28]:

url10Q = 'http://www.sec.gov/Archives/edgar/data/' + cik_nz + '/' + accno_nh + '/' + extension[0]


# In[ ]:

response10Q = http.request('GET',url10Q)
df_list = pd.read_html(response10Q.data)
path = '.\\Part1CSV\\'
os.makedirs(os.path.join(path), exist_ok=True)
i = 0
for df in df_list:
    i += 1 
    df.to_csv(os.path.join(path,str(i) + '.csv'))


# In[ ]:

shutil.make_archive('Part1CSV', 'zip', path)
shutil.rmtree(os.path.join('.\\Part1CSV')) 


# In[ ]:

file = 'Part1CSV.zip'
data = open(file, 'rb')
# S3 Connect
s3 = boto3.resource(
    's3',
    aws_access_key_id=awskey,
    aws_secret_access_key=awssecretkey,
    config=Config(signature_version='s3v4')
)
# File Upload
s3.Bucket(awsbucket).put_object(Key=file, Body=data, ACL='public-read-write')
data.close()
os.remove(os.path.join('.\\',file))
print ("Done")
