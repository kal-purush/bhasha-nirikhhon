from fastapi import FastAPI
from fastapi.params import Body

app = FastAPI()

# !!!WARNING!!!
# Fast api read the first path request if that match the next are skip

""" 
GET method
Arguments: Path to root
Returns: Python dictionnary convert to JSON
"""


@app.get('/')
def root():
    # Each time modify the return we have to relaunch uvicorn server
    # --reload allow uvicorn to watch changes
    return {"message": "Hello world!!"}


""" 
GET method
Arguments: Path to posts
Returns: Python dictionnary convert to JSON
"""


@app.get('/posts')
def method_name():
    return {"data": "This is your posts"}


""" 
POST method
Arguments: Path to posts
Returns: Python dictionnary convert to JSON
"""


@app.post('/posts')
# Stock Post data into a dictionnary variable named payLoad Body is import from Fastapi
def create_post(payLoad: dict = Body(...)):
    # display the data in uvicorn server
    print(payLoad)
    # display the data in a python dictionnary (JSON)
    return {"new_post": f"title: {payLoad['title']} content: {payLoad['content']}"}