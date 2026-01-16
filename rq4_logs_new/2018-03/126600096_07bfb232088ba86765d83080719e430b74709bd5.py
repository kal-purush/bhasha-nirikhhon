from sklearn.feature_extraction.text import HashingVectorizer
import pandas as pd
import time
import xml.etree.ElementTree as ET
import re
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
import sys
import pickle

def extract (s, targetTag):
	global dataDF
	data = ET.fromstring(s)
	Id = data.attrib['Id']
	Body = data.attrib['Body']
	Tags = None
	if('Tags' in data.attrib):
		Tags = data.attrib['Tags']
	Body = re.sub('<\\S+>','', Body)
	Body = re.sub('\n',' ', Body)
	#print (Tags)
	if (Tags):
		Tags = re.sub('<','',Tags)
		Tags = re.sub('>',',',Tags)
		Taglist = Tags.split(',')[:-1]
		if (targetTag in Taglist):
			label = 1.0
		else:
			label = 0.0
		#print ('Tags: ')
		#print (Taglist)
		dataDF = dataDF.append({'Id':Id, 'Body':Body, 'Tags':Taglist, 'Label':label}, ignore_index=True)
lines = []
num_lines = int(sys.argv[1])
i=0
with open('Posts.xml') as f:
	while(i <= num_lines):
		line = f.readline()
		lines.append(line.rstrip('\n'))
		i = i+1
	f.close()

#lines = [line.rstrip('\n') for line in open('Posts.xml')]

dataDF = pd.DataFrame(None, columns=['Id', 'Body', 'Tags', 'Label'])

targetTag = 'java'
#print (len(lines))

start_time = time.time()
for line in lines[2:num_lines-2]:
	extract(line, targetTag)
end_time = time.time()
print ('Time to extract: ' + str(end_time - start_time))

featuresDF = dataDF[['Id', 'Body', 'Tags']]
labelDF = dataDF['Label']
#print (featuresDF.shape)
#print (labelDF.shape)
#print (featuresDF)
fit_start = time.time()
tf = HashingVectorizer()
#tf.fit(featuresDF, labelDF)
#aDF = tf.transform(featuresDF['Body'])
lr = LogisticRegression(C=50, n_jobs=1)
#lr.fit(aDF, labelDF)
pipe = Pipeline([('tf', tf), ('lr', lr)])
pipe.fit(featuresDF['Body'], labelDF)
fit_end = time.time()
#s = pickle.dumps(pipe)
print ('Time to fit ' + str(fit_end - fit_start))