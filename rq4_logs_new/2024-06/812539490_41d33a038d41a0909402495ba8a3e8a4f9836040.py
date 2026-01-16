from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from todo import todo_router # added
import uvicorn

app = FastAPI()

origins = ["http://127.0.0.1:5500", "http://34.196.146.220:80"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def welcome() -> dict:
    return {
        "msg" : "hello world?"
    }

app.include_router(todo_router)  # added

if __name__ == '__main__':
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)