from fastapi import FastAPI, Request
import base64

app = FastAPI()

@app.get("/convert_to_base64")
def convert_to_base64(term):
    base64_bytes = base64.b64encode(bytes(term, 'utf-8'))
    return {"base64": base64_bytes.decode('utf-8'), "message": "Succefully converted the string"}