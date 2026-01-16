from typing import Any
from django.http import JsonResponse
from jwt import decode, exceptions

class JWTMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Verifica se a solicitação possui o cabeçalho de autenticação com o token JWT
        if 'login' not in request.path:
            try:
                if 'HTTP_AUTHORIZATION' not in request.META:
                    return JsonResponse({'error': 'Token JWT não informado'}, status=401)
                
                # Extrai o token do cabeçalho
                token = request.META['HTTP_AUTHORIZATION'].split(' ')[1]

                # Decodifica o token JWT
                payload = decode(token, 'secret_key', algorithms=['HS256'])

                # Armazena os dados do token no objeto de solicitação para uso posterior
                request.jwt_payload = payload

            except exceptions.DecodeError:
                return JsonResponse({'error': 'Token JWT inválido'}, status=401)

        response = self.get_response(request)
        return response