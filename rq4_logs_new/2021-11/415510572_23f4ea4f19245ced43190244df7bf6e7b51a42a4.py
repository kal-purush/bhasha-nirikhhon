from google.oauth2 import id_token
from google.auth.transport import requests
from django.utils.decorators import method_decorator
from rest_framework.views import APIView
from django.views.decorators.csrf import csrf_exempt
from .google_util import google_validate_id_token
from django.contrib.auth.models import User
from .models import Evaluate, Userinfo
from django.http import HttpResponse, JsonResponse
from .about_jwt import create_token, validate_token

@method_decorator(csrf_exempt, name='dispatch')
class GoogleLoginView(APIView):
	# (Receive token by HTTPS POST)
	# ...
	def post(self, request):
		try:
			# Specify the CLIENT_ID of the app that accesses the backend:
			google_token = request.headers.get('Authorization', None)
			# google_validate_id_token(google_token)
			CLIENT_ID = "506826022275-kaqkf69c5ud0kt3g98s50d8sbi7stg2f.apps.googleusercontent.com"
			idinfo = id_token.verify_oauth2_token(google_token, requests.Request(), CLIENT_ID)
			print(idinfo)
			# Or, if multiple clients access the backend server:
			# idinfo = id_token.verify_oauth2_token(token, requests.Request())
			# if idinfo['aud'] not in [CLIENT_ID_1, CLIENT_ID_2, CLIENT_ID_3]:
			#     raise ValueError('Could not verify audience.')

			# If auth request is from a G Suite domain:
			# if idinfo['hd'] != GSUITE_DOMAIN_NAME:
			#     raise ValueError('Wrong hosted domain.')

			# ID token is valid. Get the user's Google Account ID from the decoded token.
			# userid = idinfo['sub']
			login_site = 'google'
			user_id = login_site + '_' + str(idinfo.get('sub'))
			# print(user_id)
			user_nickname = idinfo.get('name')

			user, user_flag = User.objects.get_or_create(username=user_id, password=user_id, last_name=user_nickname)
			userinfo = Userinfo.objects.get_or_create(user=user, signup_site=login_site, name=user_nickname)
			encoded_jwt = create_token(user_id, user_nickname)

			return JsonResponse({'jwt': encoded_jwt}, status=200)
			
		except ValueError:
			# Invalid token
			pass
