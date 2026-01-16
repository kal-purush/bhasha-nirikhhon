import facebook
import datetime

# Generate access token at https://developers.facebook.com/tools/explorer
access_token = 'users-access-token'

graph = facebook.GraphAPI(access_token, version='2.5')

thankyou_comments = ['Thanks a lot', 'Thank you!', 'Thanks']

birthday_keywords = ['happy', 'birthday', 'bday', 'wish', 'returns', 'b\'day']

args = {'fields' : 'birthday'}
birthdate = graph.get_object('me', **args)['birthday']

