from fastapi import FastAPI
from pydantic import BaseModel
app=FastAPI()#instantiating
'''
FastAPI is a framework to simply design API's.
decorators which are made for different different opertaions .basically they present the url which will hitted from frontend.
we are using swaggerUI for operations to accessing it->localhost:8000/docs

'''
@app.get('/')
def index():
    return {'data':{"name":"vivekanand pandey"}}

@app.get('/about')
def about():
    return {"data": { "page_detail":"welcome to about page"}}


