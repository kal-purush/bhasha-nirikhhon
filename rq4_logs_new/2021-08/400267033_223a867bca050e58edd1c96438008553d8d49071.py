from fastapi import FastAPI, Request, WebSocket
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()

templates = Jinja2Templates(directory='templates')

app.mount("/static", StaticFiles(directory="static"), name="static")

FAKE_DB = [
    {'id': 1, 'data': 'Item 1'},
    {'id': 2, 'data': 'Item 2'},
    {'id': 3, 'data': 'Item 3'},
]


@app.get('/')
async def root(request: Request):
    return templates.TemplateResponse(name='index.html', context={'request': request, 'items': FAKE_DB})


@app.websocket('/websock')
async def root(websocket: WebSocket):
    await websocket.accept()
    while True:
        message = await websocket.receive_json()
        if message.get('data'):
            await websocket.send_json({"id": int(message.get('id')) + 1, "data": message.get('data')})