from fastapi import FastAPI
from Model import *
from Service import *

app = FastAPI()

@app.post("/register")
def PostRegister():
    raise NotImplementedError

@app.post("/login")
def PostRegister():
    raise NotImplementedError

@app.get("/logout")
def PostRegister():
    raise NotImplementedError

@app.get("/rollcall")
def PostRegister():
    raise NotImplementedError

@app.post("/rollcall")
def PostRegister():
    raise NotImplementedError

@app.post("/googleoauth") # Not sure method
def PostRegister():
    raise NotImplementedError