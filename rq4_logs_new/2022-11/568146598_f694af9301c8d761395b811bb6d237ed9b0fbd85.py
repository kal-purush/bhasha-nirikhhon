from fastapi import FastAPI, Query

from expense_tracker.fake_db import user_list

app = FastAPI()


@app.get("/healthcheck/")
async def healthcheck() -> dict:
    return {"message": "ok"}


@app.get("/users/")
async def users() -> dict[int:dict]:
    return user_list


@app.get("/users/{key}/")
async def user_by_key(
    key: int = 0, params: list[str] | None = Query(default=None)
) -> dict | str:
    if params:
        return {key: user_list[key].get(param) for param in params}
    return user_list[key]


@app.post("/users/create/")
async def create_user(name: str, surname: str) -> dict:
    user = {"name": name, "surname": surname}
    new_key = max(user_list.keys()) + 1
    user_list[new_key] = user
    return user