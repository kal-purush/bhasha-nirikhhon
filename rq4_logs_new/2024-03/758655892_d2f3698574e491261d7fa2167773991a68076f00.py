from fastapi import FastAPI
from pydantic import BaseModel
app = FastAPI()

#inicia el server : uvicorn users:app --reload
#inicia el server : uvicorn main:app --reload

# Entidad user

class User(BaseModel):
    id: int
    #name: str
    surname: str
    url: str
    age: int

users_list = [User(id=1, name="Brais", surname="moure", url="https://moure.dev", age=35), 
            User(id=2, name="Moure", surname="Dev", url="https://mouredev.com", age=35),
            User(id=3, name="Haakon", surname="Dahlberg", url="https://Haakon.com", age=33)]

@app.get("/usersjson")
async def usersjson():
    return [{"name": "Brais", "surname": "moure", "url": "https://moure.dev", "age": 35},
            {"name": "Moure", "surname": "Dev", "url": "https://mouredev.com" ,"age": 35},
            {"name": "Haakon", "surname": "Dahlberg", "url": "https://Haakon.com", "age": 33}]

@app.get("/users")
async def users():
    return users_list

@app.get("/user/{id}")
async def user(id: int):
    users = filter(lambda user: user.id == id, users_list)
    try:
        return list(users)[0]
    except:
        return {"error": "no se ha encontrado el usuario"}

#@app.post()