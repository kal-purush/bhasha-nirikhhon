from fastapi import FastAPI
from pydantic import BaseModel
from typing import Text, Optional
from datetime import datetime
from uuid import uuid4 as uuid


app= FastAPI()

posts = []
#Post model

class Post(BaseModel):
    id: Optional[str]
    title: str
    author: str
    content: Text
    create_at: datetime = datetime.now()
    published_at: Optional[datetime] 
    published: bool = False


@app.get('/')
def read_root():
    return {"welcome": "welcome to my REst API"}


@app.get('/posts')
def get_posts():
    return posts

@app.post('/posts')
def save_post(post: Post):
    post.id = str(uuid())
    posts.append(post.dict())
    return "Recibido" 