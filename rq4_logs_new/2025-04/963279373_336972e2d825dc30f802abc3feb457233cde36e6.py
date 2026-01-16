from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


class Memo(BaseModel):
    id:str
    content:str

memos = []

app = FastAPI()

@app.post("/memos")
def create_memo(memo:Memo):
    memos.append(memo)
    return "추가 성공이요."

@app.get("/memos")
def read_memo():
    
    return sorted(memos, key=lambda x: (x.content, x.id))

@app.put("/memos/{memo_id}")
def put_memo(req_memo:Memo):
    for memo in memos:
        if memo.id==str(req_memo.id):
            memo.content = req_memo.content
            return '변경완료요.'
    return "xXXXXX"

@app.delete("/memos/{memo_id}")
def delete_memo(memo_id):
    for index,memo in enumerate(memos):
        if memo.id==str(memo_id):
            memos.pop(index)
            return '성공했습니다.'
    return "xXXXXX"

app.mount("/", StaticFiles(directory='static',html=True), name='static')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080)