from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
import uvicorn

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get("/")
async def root():
    return templates.TemplateResponse("index.html", {"message": "Hey, it's a service for the recommendation of movies."})


if __name__ == "__main__":
    uvicorn.run("server:app", host="127.0.0.1", port=8000, log_level="info")