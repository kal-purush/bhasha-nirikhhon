from fastapi import FastAPI
from typing import Optional
from pydantic import BaseModel
'''
FastAPI is a framework to simply design API's.
decorators which are made for different different opertaions .basically they present the url which will hitted from frontend.
we are using swaggerUI ,for operations to accessing it->localhost:8000/docs
for running the file we use uvicorn main:app,here unicorn is the universal command,main is the file name 
            app is the instance of fastAPI. for reloading after commnad --reload.
changing of the name of the function not effect the working of the app.
if the name of the function for different operation will same, that not effect the working of the path operation function.

for dynamic url we use {} and to set datatype in prefix  we use (:int,:str,bool,..)
url is of type string means in dynamic url whatever will thrown will be of type string.
fastAPI read line by line ex->if two functions having same operation but slight change in url will execute the first coming operation. 
so all datatype validation is covered by pydantic 

like swaggerUI fastAPI is having somthing for reading the documentation and showing the route .in URL-> localhost/redoc,it will show all routes on left side
pathparameter and queryparaetr are two different things.Path parameter exists in url and not contained ? with it whereas.
Quryparameter comes with ? mark .when we want to limit our data while fetcing for efficiency of Database we use queryparameter. 
like dynamic url we have accept the queryparameter.in function we use limit keyword
if two queryparameter are given and from browser only one parameter is fired then it will shoe error and demand for another parameter,to bypass this problem we gave default values
ex->def index(limit:int=10,published:bool=True):
sometimes we use optional imported from typing.we use it when we can get any parameter or not get any parameter,also in that case we have to use default value.
'''
'''
from browser we cannot send data thats when request body is needed.for that we need pydantic models.
we inherit BaseModel of pydantic and make a schema ,schema can have optional field.scema will field :data_type.
we can change the port on which our app is runnning .for that __name__=="__main__" will checked and uvicorn take parameter(app,host="127.0.0.1",port=9000)
FastAPI dont use a SQL(relational databases)

'''

app=FastAPI()#instantiating
@app.get('/')
def index(limit:int=10,published:bool=True,sort=None):
    if published :
        return {'data':f"{limit} blogs from th db"}
    else:
        return {'data':f'blogs are not published'}    

@app.get('/blog/{id}')
def about(id:int):
    return {"data": id}






class Blog(BaseModel):
    title:str
    body:str
    published:Optional[bool]

@app.post('/blog')
def create_blog(blog:Blog):
    # print(blog)
    return {"data":f"In blog title is {blog.title}"}
