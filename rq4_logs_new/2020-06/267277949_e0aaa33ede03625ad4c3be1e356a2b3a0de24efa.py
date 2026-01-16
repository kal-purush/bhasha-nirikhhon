import hashlib
import db
from models import User
from starlette.status import HTTP_401_UNAUTHORIZED
from fastapi import HTTPException

def auth(credentials):
    username = credentials.username
    password = hashlib.md5(credentials.password.encode()).hexdigest()
    user = db.session.query(User).filter(User.username == username).first()
    db.session.close()

    if user is None or user.password != password:
        error = 'ユーザ名かパスワードが間違っています．'
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail=error,
            headers={"WWW-Authenticate":"Basic"},
        )
    return username