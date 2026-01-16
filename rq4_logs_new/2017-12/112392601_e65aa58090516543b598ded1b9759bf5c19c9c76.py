import urllib2, time
from twilio.rest import Client # pip install twilio

account_sid = 'your_account_sid'
auth_token  = 'your_auth_token'
client = Client(account_sid, auth_token)

def WebData(): 
 timeT = 0 
 length8 = '' 
 while timeT < 1: 
  url = urllib2.urlopen('https://github.com/') 
  data = url.read() 
  datastring = str(data) 
  lengthA = len(datastring)
  print (lengthA)
  if length8 != '' and lengthA != length8:
   message = client.messages.create(
    to='#', 
    from_='#',
    body='The website Has changed from ' + str(lengthA) + ' to ' + str(length8) + 'characters')
   print(message.sid)
   print ('The website Has changed from ' + str(lengthA) + ' to ' + str(length8) + 'characters')
   return
  else:
   length8 = lengthA
  time.sleep(5)
   
WebData()