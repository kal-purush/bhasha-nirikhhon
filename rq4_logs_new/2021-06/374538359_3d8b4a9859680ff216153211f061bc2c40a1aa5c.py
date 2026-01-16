import json, jwt

from django.http import JsonResponse

from .models          import User
from gonggol.settings import SECRET_KEY, ALGORITHM

def UserInfoDeco(func):
    def wrapper(self, request, *args, **kwargs):
        try:
            token              = request.headers.get('authorization', None)
            payload            = jwt.decode(token, SECRET_KEY, algorithms=ALGORITHM)
            user               = User.objects.get(account=payload['account'])
            request.login_user = user

            return func(self, request, *args, **kwargs)

        except jwt.exceptions.DecodeError:
            return JsonResponse({'message':'INVALID_TOKEN'},status=400)

        except User.DoesNotExist:
            return JsonResponse({'message':'INVALID_USER'},status=400)
            
    return wrapper