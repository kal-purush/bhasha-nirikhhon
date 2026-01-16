from functools import wraps

from fastapi import Security, HTTPException
from fastapi_jwt import JwtAuthorizationCredentials

from src.common.application.user_role import UserRole
from src.common.infrastructure.token.access_security import access_security


def admin_only(func):
    @wraps(func)
    def wrapper(credentials: JwtAuthorizationCredentials = Security(access_security), *args, **kwargs):
        if credentials["role"] != UserRole.ADMIN:
            raise HTTPException(status_code=403)
        return func(credentials=credentials, *args, **kwargs)
    return wrapper


def psychologist_only(func):
    @wraps(func)
    def wrapper(credentials: JwtAuthorizationCredentials = Security(access_security), *args, **kwargs):
        if credentials["role"] != UserRole.PSYCHOLOGIST:
            raise HTTPException(status_code=403)
        return func(credentials=credentials, *args, **kwargs)
    return wrapper


def admin_only_async(func):
    @wraps(func)
    async def wrapper(credentials: JwtAuthorizationCredentials = Security(access_security), *args, **kwargs):
        if credentials["role"] != UserRole.ADMIN:
            raise HTTPException(status_code=403)
        return await func(credentials=credentials, *args, **kwargs)
    return wrapper


def psychologist_only_async(func):
    @wraps(func)
    async def wrapper(credentials: JwtAuthorizationCredentials = Security(access_security), *args, **kwargs):
        if credentials["role"] != UserRole.PSYCHOLOGIST:
            raise HTTPException(status_code=403)
        return await func(credentials=credentials, *args, **kwargs)
    return wrapper