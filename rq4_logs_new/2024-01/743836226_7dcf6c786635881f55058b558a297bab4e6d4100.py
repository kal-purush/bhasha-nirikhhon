from datetime import datetime
from enum import Enum
from typing import List, Optional

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import ResponseValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

app = FastAPI(
    title="Bulletin board"
)


@app.exception_handler(ResponseValidationError)
async def validation_exception_handler(request: Request, exc: ResponseValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
    )



users = [
    {"id": 1, "user_id": 2, "role": "admin", "username": "Bob1"},
    {"id": 2, "user_id": 2, "role": "user", "username": "Bob2"},
    {"id": 3, "user_id": 1, "role": "user", "username": "Bob3"},
    {"id": 4, "user_id": 2, "role": "user", "username": "Bob4"},
    {"id": 5, "user_id": 1, "role": "user", "username": "Bob5", "degree": [
        {"id": 1, "created_at": "2024-01-16T09:06:43.439Z", "type_degree": "opt"}
    ]},
]


class DegreeType(Enum):
    opt = "opt"
    opt1 = "opt1"


class Degree(BaseModel):
    id: int
    created_at: datetime
    type_degree: DegreeType


class User(BaseModel):
    id: int
    username: str
    role: str
    degree: Optional[List[Degree]] = None


@app.get("/users/{user_id}", response_model=List[User])
async def get_user(user_id: int):
    return [user for user in users if user.get("id") == user_id]


users2 = [
    {"id": 1, "user_id": 2, "role": "admin", "username": "Bob"},
    {"id": 2, "user_id": 2, "role": "user", "username": "Bob2"},
    {"id": 3, "user_id": 1, "role": "user", "username": "Bob3"},
    {"id": 4, "user_id": 2, "role": "user", "username": "Bob4"},
    {"id": 5, "user_id": 1, "role": "user", "username": "Bob5", "degree": [
        {"id": 1, "created_at": "2024-01-01T00:00:00", "type_degree": "opt"}
    ]},
]


@app.get("/fake_users")
def get_fake_users(limit: int = 3, offset: int = 1):
    return users2[offset:][:limit]


users3 = [
    {"id": 1, "user_id": 2, "role": "admin", "name": "Bob"},
    {"id": 2, "user_id": 2, "role": "user", "name": "Bob2"},
    {"id": 3, "user_id": 1, "role": "user", "name": "Bob3"},
    {"id": 4, "user_id": 2, "role": "user", "name": "Bob4"},
    {"id": 5, "user_id": 1, "role": "user", "name": "Bob5"},
]


@app.post("/user_change/{user_id}")
def post_username_change(user_id: int, new_name: str):
    current_user = list(filter(lambda user: user.get("id") == user_id, users3))[0]
    current_user["name"] = new_name
    return {"status": 200}


fake_ads = [
    {"id": 1, "user_id": 2, "text": "text ads1", "price": 2.2},
    {"id": 2, "user_id": 2, "text": "text ads2", "price": 3.2},
    {"id": 3, "user_id": 1, "text": "text ads3", "price": 4.2},
    {"id": 4, "user_id": 1, "text": "text ads4", "price": 5.2},
    {"id": 5, "user_id": 2, "text": "text ads5", "price": 6.2},
]


class Ads(BaseModel):
    """
    Валидация данных с помощью Pydantic
    """
    id: int
    user_id: int
    text: str = Field(max_length=500)
    price: float = Field(ge=0)


@app.post("/ads")
def add_ads(new_ads: List[Ads]):
    fake_ads.extend(new_ads)
    return {"status": 200, "data": fake_ads}