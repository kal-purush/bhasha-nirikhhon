import uvicorn
from fastapi import FastAPI

from fermentation_station.routers import temp, root, last_action

app = FastAPI()

app.include_router(temp.router)
app.include_router(root.router)
app.include_router(last_action.router)


def main():
    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()