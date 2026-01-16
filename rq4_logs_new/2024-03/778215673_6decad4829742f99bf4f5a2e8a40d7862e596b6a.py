import os
import requests
from django.http import JsonResponse
from rest_framework.response import Response
from rest_framework import status
from decouple import config

def get_access_token(request):
    consumer_key = config("consumer_key",'')
    consumer_secret = config("consumer_secret")
    access_token_url = 'https://sandbox.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials'
    headers = {'Content-Type': 'application/json'}
    auth = (consumer_key, consumer_secret)

    try:
        response = requests.get(access_token_url, headers=headers, auth=auth)
        response.raise_for_status()
        result = response.json()
        access_token = result['access_token']
        return JsonResponse({'access_token': access_token}, status=status.HTTP_200_OK)
    except requests.exceptions.RequestException as e:
        return JsonResponse({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)