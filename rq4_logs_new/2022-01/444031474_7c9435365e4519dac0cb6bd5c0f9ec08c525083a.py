from typing import Optional

from pydantic import BaseModel, Field, EmailStr


class UserSchema(BaseModel):
    email: EmailStr = Field(...)
    password: str = Field(...)

    class Config:
        schema_extra = {
            "example": {
                "email": "example@example.com",
                "password": "daswef123@#2"
            }
        }


class UpdateUserModel(BaseModel):
    email: Optional[str]
    password: Optional[str]

    class Config:
        schema_extra = {
            "example": {
                "email": "example@example.com",
                "password": "daswef123@#2"
            }
        }
