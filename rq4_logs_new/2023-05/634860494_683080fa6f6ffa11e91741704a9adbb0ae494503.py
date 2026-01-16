"""
Foodindo App helps to automate lunch registration
and validation for Entri Office Lunch
"""
import os
from os.path import exists
from dotenv import load_dotenv
import pyqrcode
from fastapi import FastAPI, APIRouter
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware
import aioredis

load_dotenv(".env")

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter()

REDIS_URL: str = os.getenv("REDIS_URL")
HOST: str = os.getenv("HOST")
redis = aioredis.from_url(REDIS_URL, decode_responses=True)

@app.get("/")
async def root() -> JSONResponse:
    """
    Index Page
    """
    user_ids: list = [1,2,3]
    removed: bool = bool(await redis.delete('test'))
    response: int = await redis.sadd('test', *user_ids)
    value: list = await redis.smembers('test')
    count: int = await redis.scard('test')
    return {
        "message": value,
        "count": count,
        "response": response,
        "is_removed": removed
    }

@router.get("/user/{user_id}/register")
async def register_user(user_id: str) -> JSONResponse:
    """
    Takes daily registrations from
    SlackBot integration
    """
    is_added: int = await redis.sadd('members', user_id)
    _: int = await redis.sadd('validator', user_id)
    qr_code_path: str = f"media/{user_id}.png"
    qr_code_file_path: str = f"{HOST}/{qr_code_path}"
    if exists(qr_code_path):
        return JSONResponse(content={
            "data": f"{user_id} registered",
            "qr_code_url": qr_code_file_path
        },
        status_code=200)
    if not is_added:
        return JSONResponse(
            content={
                "error": "User already registered",
                "qr_code_url": qr_code_file_path
            },
            status_code=400)
    qr_code_url: str = f"{HOST}/api/v1/user/{user_id}/validate"
    qr_code: pyqrcode = pyqrcode.create(qr_code_url)
    qr_code.png(qr_code_path, scale = 6)
    return JSONResponse(content={
        "data": f"{user_id} registered",
        "qr_code_url": qr_code_file_path
    },
    status_code=200)


@router.get("/user/{user_id}/validate")
async def validate_user(user_id: str) -> JSONResponse:
    """
    Sample url with path
    """
    is_member: int = await redis.sismember('members', user_id)
    if not is_member:
        return JSONResponse(
            content={"error": "User not registered"},
            status_code=400)
    count: int = await redis.srem('validator', user_id)
    if not count:
        return JSONResponse(
            content={"error": "User already validated"},
            status_code=400)
    return JSONResponse(
        content={"data": "User Validated"},
        status_code=200)

@router.get("/total-registrations")
async def total_registrations() -> JSONResponse:
    """
    Returns the count of total user registrations
    for a day
    """
    total_count: int = await redis.scard('members')
    members: list = list(await redis.smembers('members'))
    return JSONResponse(content={
        'total_count': total_count,
        'members': members
    },
    status_code=200)