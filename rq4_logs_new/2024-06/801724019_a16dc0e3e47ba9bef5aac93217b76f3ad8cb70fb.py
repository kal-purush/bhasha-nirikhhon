import logging

from common.data_handlers.user_handler import UserHandler
from common.helpers.auth import get_pwd_hash
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from models.user import User

router = APIRouter()
logger = logging.getLogger("uvicorn.error")
user_handler = UserHandler()


@router.post("")
async def create_user(user: User):
    try:
        user.password = get_pwd_hash(user.password)
        user_handler.create_user(user)
        return JSONResponse(status_code=200, content={"message": "User created"})
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail="Failed to create user")


@router.delete("/{id}")
async def delete_user(id: int):
    try:
        user_handler.delete_user(id)
        return JSONResponse(status_code=200, content={"message": "User deleted"})
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail="Failed to delete user")