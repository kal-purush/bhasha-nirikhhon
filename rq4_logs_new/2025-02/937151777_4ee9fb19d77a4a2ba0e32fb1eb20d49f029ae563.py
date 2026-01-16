import os
import uvicorn
from fastapi import FastAPI
from routes.views import gen_seq
from dotenv import load_dotenv

load_dotenv('.env-dev')

app = FastAPI()

app.include_router(gen_seq.routher)

@app.get('/')
def hello():
    return {"message":"hello, I am EVO2"}


if __name__=='__main__':
    uvicorn.run(app=app,host=os.getenv("URL"),port=int(os.getenv("PORT")))