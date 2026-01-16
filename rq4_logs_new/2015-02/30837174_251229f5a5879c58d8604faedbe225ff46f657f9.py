import requests
import random
import json

import logging
logging.basicConfig(filename='fb.log',level=logging.INFO)
# logging.debug('This message should go to the log file')
# logging.info('So should this')
# logging.warning('And this, too')

thankyous = [
	'Thank you so much ! Hope you have a great year ahead ! :)',
	'Thanks ! God Bless ! Have a great year ! :) ', 
	'Thank you ! Have a great year ! tc :)', 
	'Thanks alot ! Have an awesome new year ! :)',
	'Thank you ! God Bless ! Have fun ! :)',
    'Thank you ! Have an amazing year ! :) '
	]

message = thankyous[random.randint(0,len(thankyous))]

key='CAAEmsGgBchQBAKD2zLTrIDePnyhOuubEi07k2rQ4226mgmMMAvRPYc5mTVmfSbkfSEY8lR7rZAv21lxktqE5Ou0iWsvGC4DthjIo3sLVNZBQMhYz9cZAPHOa3EmZADgtkRNY18hBqA0Al55HKkLCZC3Vj6ZBZAZA9Wd4VPqAAArMsak6x4a09Y0Jol8ZBZC3A2xybGstoq5ngoI0CxyFaJcmn19CLHGhdvtxcZD'

link='https://graph.facebook.com/fql'
# query ='SELECT message,post_id,created_time,permalink FROM stream where source_id=me() and actor_id!=me() and created_time >= 1419930773 and created_time <= 1419936284 LIMIT 500'
query ='SELECT message,post_id,actor_id,created_time,permalink FROM stream where source_id=me() and actor_id!=me() and created_time >= 1419762700 and created_time <= 1419935500 LIMIT 500'

para = {'q':query,'access_token':key}
feed = requests.get(link,params=para)
# print(feed)
# print(feed.json())
record = feed.json()
number_posts = len(record['data'])
print "Total number of posts = %s"%(number_posts)
# for i in range(0,100):
	# import pdb; pdb.set_trace()
	# post_id = record['data'][i]['post_id']
	# pid = post_id.split('_')
	# objId = pid[1]
	# print(objId)
	# actor_id = 
	# print "Name:- "
	# send={'access_token':key,'message':'Thank you so much !! :) God Bless !! :)'}
	# comment = requests.post('https://graph.facebook.com/v2.0/'+str(post_id)+'/comments?access_token='+key,params=send)
	# print('For'+ str(post_id) +' Comment'+ str(comment))
	# print str(comment.content)
for post in record['data']:
	actor_id = post['actor_id']
	post_id = post['post_id']
	actor_req = requests.get('https://graph.facebook.com/%s'%(actor_id))
	actor_json = json.loads(actor_req.content)
	print "%s : %s"%(actor_json['first_name'], post['message'])
	thanks_len = len(thankyous)
	message = thankyous[random.randint(0,thanks_len-1)]
	custom_message = " Hey %s ! %s"%(actor_json['first_name'],str(message))
	send={'access_token':key,'message':str(custom_message)}
	comment = requests.post('https://graph.facebook.com/v2.0/'+str(post_id)+'/comments?access_token='+key,params=send)
	logging.info(actor_json['first_name'])
	# import pdb; pdb.set_trace()
	if comment.status_code == 200:
		logging.info(str("Posted !!"))
		print "====> %s"%(custom_message)
	else:
		logging.warning(str(comment.content))
	# logging.info()