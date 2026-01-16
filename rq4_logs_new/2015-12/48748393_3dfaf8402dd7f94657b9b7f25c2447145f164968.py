import sqlite3
import requests
import json

_BASE_ADDRESS='https://api.telegram.org/bot'
_DB_NAME="Pytbot_data.db"
_time_out=None
_chunk_size = 2048





###################### GLOBAL INTERNAL FUNCTIONS ##############################
def init_lib():
	con=sqlite3.connect(_DB_NAME)
	curs=con.cursor()
	curs.execute("CREATE TABLE IF NOT EXISTS bots (bot_token CHAR(55) PRIMARY KEY NOT NULL, upid CHAR(12) )")
	con.commit()
	con.close()
def _validate_response_msg(response_str):
	if(not response_str):
		return False
	response = json.loads(response_str)
	if(not response['ok']):
		return response['result']
	return message(response['result'])
	
def _validate_response_bool(response_str):
	if(not response_str):
		return False
	response = json.loads(response_str)
	if(not response['ok']):
		return False
	return True
def _validate_response_raw(response_str):	
	if(not response_str):
		return False
	response = json.loads(response_str)
	return response['result']
	
	
	
init_lib()
############################## CLASSES #############################
class bot:
	def __init__(self,token,name='my_bot'):
		self._token=token
		self._update_id=0
		self.name=name
		self._db_insert_me()
	######################## INTERNAL METHODS ##########################
	def _command(self,cmd,para={},method='get',time_out=_time_out):
		address=_BASE_ADDRESS+self._token+"/"+cmd
		
		if(method=='get'):
			
			try:
				req=requests.get(address,params=para,timeout=time_out)
			except requests.exceptions.Timeout:
				return None
				
		elif(method=='post'):
			try:
				req=requests.post(address,data=para,timeout=time_out)
			except requests.exceptions.Timeout:
				return None
		else:
			raise Exception("{} is not a valid method".format(method))
				
				
		if(req.status_code==200):
			return req.text
		elif(req.status_code==404):
			raise Exception("No such Command")
		else:
			raise Exception("HTTP ERROR "+str(req.status_code) +"\n" + req.text)
	
	def _command_upload(self,cmd,file_type,file_add,para=None,time_out=_time_out):
		address=_BASE_ADDRESS+self._token+"/"+cmd
		try:
			req=requests.post(address,data=para,timeout=time_out,files={file_type: open(file_add, 'rb')})
		except requests.exceptions.Timeout:
			return None
		
		
		if(req.status_code==200):
			return req.text
		elif(req.status_code==404):
			raise Exception("No such Command")
		else:
			raise Exception("HTTP ERROR "+str(req.status_code)+"\n" + req.text)
			
			
	def _db_set_update_id(self,upid):
		if(upid>self._update_id):
			con=sqlite3.connect(_DB_NAME)
			curs=con.cursor()
			curs.execute("UPDATE bots SET upid=? WHERE bot_token=?",(upid,self._token))
			con.commit()
			con.close()
			
	def _db_get_update_id(self):
		con=sqlite3.connect(_DB_NAME)
		curs=con.cursor()
		curs.execute("SELECT * FROM bots WHERE bot_token=?",(self._token,))
		row=curs.fetchone()
		self._update_id=int(row[1])
		con.close()
	
	def _db_insert_me(self):
		con=sqlite3.connect(_DB_NAME)
		curs=con.cursor()
		curs.execute("INSERT OR IGNORE INTO bots VALUES (?,?)", (self._token,1))
		con.commit()
		con.close()
	
	
	######################### API METHODS ######################
	def getMe(self):
		''' returns User Object '''
		response_str = self._command('getMe')
		if(not response_str):
			return False
		response = json.loads(response_str)	
		return response
	
	def sendMessage(self,chat_id,text,parse_mode=None,disable_web=None,reply_msg_id=None,markup=None):
		''' On failure returns False
		On success returns Message Object '''
		payload={'chat_id' : chat_id, 'text' : text, 'parse_mode': parse_mode , 'disable_web_page_preview' : disable_web , 'reply_to_message_id' : reply_msg_id}
		
		if(markup):
			payload['reply_markup']=json.dumps(markup)
			
		response_str = self._command('sendMessage',payload,method='post')
		
		return _validate_response_msg(response_str)
	
	def forwardMessage(self,chat_id,from_chat_id,message_id):
		payload={'chat_id' : chat_id, 'from_chat_id' : from_chat_id , 'message_id':message_id }
		response_str = self._command('forwardMessage',payload)

		return _validate_response_msg(response_str)
		
	def sendPhoto(self,chat_id,photo_id=None,photo_path=None,caption=None,reply_msg_id=None,markup=None):
		payload={'chat_id' : chat_id,  'caption' : caption , 'reply_to_message_id' : reply_msg_id }
		if(markup):
			payload['reply_markup']=json.dumps(markup)
		
		if(photo_id):# RESENDING A PHOTO ON TELEGRAM SERVERS	
			payload['photo'] = photo_id
			response_str = self._command('sendPhoto',payload,method='post')
		elif(photo_path): # UPLOADING A NEW PHOTO 
			response_str = self._command_upload('sendPhoto','photo',photo_path,para=payload)
		else:	
			return False
			
		return _validate_response_msg(response_str)
			
	def sendAudio(self,chat_id,audio_id=None,audio_path=None,duration=None,performer=None,title=None,reply_msg_id=None,markup=None):
		payload={'chat_id' : chat_id,  'duration' : duration ,'performer' : performer, 'title' : title, 'reply_to_message_id' : reply_msg_id }
		if(markup):
			payload['reply_markup']=json.dumps(markup)
		
		if(audio_id):# RESENDING A FILE ON TELEGRAM SERVERS	
			payload['audio'] = audio_id
			response_str = self._command('sendAudio',payload,method='post')
		elif(audio_path): # UPLOADING A NEW FILE 
			response_str = self._command_upload('sendAudio','audio',audio_path,para=payload)
		else:	
			return False
		
		return _validate_response_msg(response_str)
		
	def sendDocument(self,chat_id,document_id=None,document_path=None,reply_msg_id=None,markup=None):
		payload={'chat_id' : chat_id, 'reply_to_message_id' : reply_msg_id }
		if(markup):
			payload['reply_markup']=json.dumps(markup)
		
		if(document_id):# RESENDING A FILE ON TELEGRAM SERVERS	
			payload['document'] = document_id
			response_str = self._command('sendDocument',payload,method='post')
		elif(document_path): # UPLOADING A NEW FILE 
			response_str = self._command_upload('sendDocument','document',document_path,para=payload)
		else:	
			return False
		
		return _validate_response_msg(response_str)
		
	def sendSticker(self,chat_id,sticker_id=None,sticker_path=None,reply_msg_id=None,markup=None):
		payload={'chat_id' : chat_id, 'reply_to_message_id' : reply_msg_id }
		if(markup):
			payload['reply_markup']=json.dumps(markup)
		
		if(sticker_id):# RESENDING A FILE ON TELEGRAM SERVERS	
			payload['sticker'] = sticker_id
			response_str = self._command('sendSticker',payload,method='post')
		elif(sticker_path): # UPLOADING A NEW FILE 
			response_str = self._command_upload('sendSticker','sticker',sticker_path,para=payload)
		else:	
			return False
		
		return _validate_response_msg(response_str)
		
	def sendVideo(self,chat_id,video_id=None,video_path=None,duration=None,caption=None,reply_msg_id=None,markup=None):
		payload={'chat_id' : chat_id,'duration' : duration, 'caption' : caption, 'reply_to_message_id' : reply_msg_id , 'title': "ast.mp4" }
		if(markup):
			payload['reply_markup']=json.dumps(markup)
		
		if(video_id):# RESENDING A FILE ON TELEGRAM SERVERS	
			payload['video'] = video_id
			response_str = self._command('sendVideo',payload,method='post')
		elif(video_path): # UPLOADING A NEW FILE 
			response_str = self._command_upload('sendVideo','video',video_path,para=payload)
		else:	
			return False
		
		return _validate_response_msg(response_str)
		
	def sendVoice(self,chat_id,voice_id=None,voice_path=None,duration=None,reply_msg_id=None,markup=None):
		payload={'chat_id' : chat_id,'duration' : duration, 'reply_to_message_id' : reply_msg_id }
		if(markup):
			payload['reply_markup']=json.dumps(markup)
		
		if(voice_id):# RESENDING A FILE ON TELEGRAM SERVERS	
			payload['voice'] = voice_id
			response_str = self._command('sendVoice',payload,method='post')
		elif(voice_path): # UPLOADING A NEW FILE 
			response_str = self._command_upload('sendVoice','voice',voice_path,para=payload)
		else:	
			return False
		
		return _validate_response_msg(response_str)
		
	def sendLocation(self,chat_id,latitude,longitude,reply_msg_id=None,markup=None):
		payload={'chat_id' : chat_id,'latitude' : latitude,'longitude' : longitude, 'reply_to_message_id' : reply_msg_id }
		if(markup):
			payload['reply_markup']=json.dumps(markup)
		response_str = self._command('sendLocation',payload,method='post')
		return _validate_response_msg(response_str)
		
	def sendChatAction(chat_id,action):
		payload={'chat_id' : chat_id,'action' : action}
		response_str = self._command('sendChatAction',payload)
		return _validate_response_bool(response_str)
		
	def getUserProfilePhotos(user_id,offset=None,limit=None):
		payload={'user_id' : user_id,'offset' : offset, 'limit' : limit}
		response_str = self._command('getUserProfilePhotos',payload)
		return _validate_response_raw(response_str)
		
	def getUpdates(self,limit=20,auto_upid=True):
		
		if(self._update_id==0):
			self._db_get_update_id()
		offset=self._update_id+1
		response_str = self._command('getUpdates',{'limit':limit,'offset':offset})
		
		if(not response_str):
			return []
		response = json.loads(response_str)
		if(not response['ok']):
			return []
		updates=[{'upid':update.get('update_id') , 'message': message(update.get('message'))}   for update in response['result'] ]
		
		if(auto_upid):
			max_id=0
			for update in updates:
				mupid=int(update.get('upid'))
				if(max_id<mupid):
					max_id=mupid
			self.set_upid(max_id)
			
		return updates
		
	def getUpdates_raw(self,limit=20):
		
		if(self._update_id==0):
			self._db_get_update_id()
		offset=self._update_id+1
		response_str = self._command('getUpdates',{'limit':limit,'offset':offset})
		
		if(not response_str):
			return []
		response = json.loads(response_str)
		if(not response['ok']):
			return []
		updates=response['result']
		return updates

	def getUpdates_iter(self,limit=20):
		
		if(self._update_id==0):
			self._db_get_update_id()
		offset=self._update_id+1
		response_str = self._command('getUpdates',{'limit':limit,'offset':offset})
		
		if(not response_str):
			return None
		response = json.loads(response_str)
		if(not response['ok']):
			return None
		updates=response['result'] 
		for update in updates:
			upid=update.get('update_id')
			msg=update.get('message')
			self.set_upid(upid)
			if(msg):
				yield message(msg)
			else:
				return None
		
	



	def getFile_path(file_id):
		payload={'file_id' : file_id}
		response_str = self._command('getFile',payload)
		if(not response_str):
			return None
		response = json.loads(response_str)
		if(not response['ok']):
			return None
		file_path=response['result'].get('file_path')
		return "https://api.telegram.org/file/bot"+self.token+"/"+file_path
		
	def getFile_dl(file_id,path=""):
		f_path=self.getFile_path(file_id)
		if(f_path):
			dl =  requests.get(f_path, stream=True,timeout=_time_out)
			if(req.status_code==200):
				with open(path+f_path[-10:], 'wb') as f:
					for chunk in dl.iter_content(_chunk_size):
						f.write(chunk)
		return False

	######################## MISC BOT ACTIONS #############################
	def set_upid(self,upid=None):
		if(upid!=None):
			self._db_set_update_id(upid)
		else:
			self._db_set_update_id(self._update_id+1)

		
		
		
		
######################## MISC API DEFINITIONS   ######################################


		
class replyMarkup:
	def __init__(self,keys=None,one_time=None,resize=None,selective=None):
		self.keyboard=None
		self.resize_keyboard=resize
		self.one_time_keyboard=one_time
		self.selective=selective
		if(keys):
			self.set_keys(keys)
		
		
	def get_dict(self):
		if(self.keyboard==None):
			return None
		ans={'keyboard': self.keyboard}
		if(self.resize_keyboard ==True):
			ans['resize_keyboard']=True
		if(self.one_time_keyboard ==True):
			ans['one_time_keyboard']=True
		if(self.selective ==True):
			ans['selective']=True
			
		return ans
		
	def set_keys(self,keys,force_row=None):
		lin=len(keys)
		if(lin==0 or force_row==0):
			self.keyboard=None
			return
			
		if(force_row==None):
			if(lin<=3):
				row=1
			elif(lin<=8):
				row=2
			else:
				row=3
		else:
			row=force_row
		
		self.keyboard=[]
		for i in range(row):
			self.keyboard.append( [] )
		k=0
		if(lin%row==0):
			step=lin//row
		else:
			step=lin//row+1
		while(k<lin):
			self.keyboard[k//step].append(keys[k])
			k+=1
		return True



###################    TELEGRAM TYPE DEFINITIONS   ##########################

class user:
	def __init__(self,para):
		self.id = para.get('id')
		self.first_name = para.get('first_name')
		self.last_name = para.get('last_name')
		self.username = para.get('username')
		
class chat:
	def __init__(self,para):
		self.id = para.get('id')
		self.type = para.get('type')
		self.title = para.get('title')
		self.username = para.get('username')
		self.first_name = para.get('first_name')
		self.last_name = para.get('last_name')
		
class message:
	def __init__(self,para):
		if(not para):
			return None
			
		self.id = para.get('message_id')
		self.date = int(para.get('date'))
		self.chat=chat(para.get('chat'))
		self.caption=para.get('caption')
		self.type=None	
		
		# Sender Handling
		self.sender=None
		tmp_sender=para.get('from')
		if(tmp_sender):
			self.sender=user(tmp_sender)
		
		# Handle TEXT MSG
		self.text=para.get('text')
		if 'text' in para:	
			self.type='text'
			self.content=self.__text2byte()
		# Handle AUDIO MSG
		if 'audio' in para:
			self.type='audio'
			self.content=t_audio(para.get('audio'))
		# Handle FILE MSG
		if 'document' in para:
			self.type='file'
			self.content=t_file(para.get('documet'))
		# Handle PHOTO MSG	
		if 'photo' in para:
			self.type='photo'
			self.content=[t_photo(w) for w in para.get('photo')]
		# Handle STICKER MSG
		if 'sticker' in para:
			self.type='sticker'
			self.content=t_sticker(para.get('sticker'))
		# Handle VIDEO MSG
		if 'video' in para:
			self.type='video'
			self.content=t_video(para.get('video'))	
		# Handle VOICE MSG
		if 'voice' in para:
			self.type='voice'
			self.content=t_voice(para.get('voice'))	
		# Handle CONTACT MSG
		if 'contact' in para:
			self.type='contact'
			self.content=t_contact(para.get('contact'))
		# Handle LOCATION MSG
		if 'location' in para:
			self.type='location'
			self.content=t_location(para.get('location'))
		# Handle SERVICE MSG
		# TODO
		
	def __text2byte(self):
		if(self.text):
			return str(self.text).encode('utf8')
		else:
			return None
			
class t_photo:
	def __init__(self,para):
		self.id=para.get('file_id')
		self.width=para.get('width')
		self.height=para.get('height')
		self.file_size=para.get('file_size')
		
class t_audio:
	def __init__(self,para):
		self.id=para.get('file_id')
		self.duration=para.get('duration')
		self.performer = para.get('performer')
		self.title=para.get('title')
		self.mime_type=para.get('mime_type')
		self.file_size=para.get('file_size')
	
class t_file:
	def __init__(self,para):
		self.id=para.get('file_id')
		self.name=para.get('file_name')
		self.mime_type=para.get('mime_type')
		self.file_size=para.get('file_size')
		
		self.thumb=None
		tmp_thumb=para.get('thumb')
		if(tmp_thumb):
			self.thumb=t_photo(tmp_thumb)
		
class t_sticker:
	def __init__(self,para):
		self.id=para.get('file_id')
		self.width=para.get('width')
		self.height=para.get('height')
		self.file_size=para.get('file_size')
		
		self.thumb=None
		tmp_thumb=para.get('thumb')
		if(tmp_thumb):
			self.thumb=t_photo(tmp_thumb)
			
class t_video:
	def __init__(self,para):
		self.id=para.get('file_id')
		self.width=para.get('width')
		self.height=para.get('height')
		self.duration=para.get('duration')
		self.mime_type=para.get('mime_type')
		self.file_size=para.get('file_size')
		
		self.thumb=None
		tmp_thumb=para.get('thumb')
		if(tmp_thumb):
			self.thumb=t_photo(tmp_thumb)
			
			
class t_voice:
	def __init__(self,para):
		self.id=para.get('file_id')
		self.duration=para.get('duration')
		self.mime_type=para.get('mime_type')
		self.file_size=para.get('file_size')
		
class t_contact:
	def __init__(self,para):
		self.phone_number=para.get('phone_number')
		self.first_name=para.get('first_name')
		self.last_name = para.get('last_name')
		self.user_id=para.get('user_id')

class t_location:
	def __init__(self,para):
		self.longitude=para.get('longitude')
		self.latitude=para.get('latitude')
		