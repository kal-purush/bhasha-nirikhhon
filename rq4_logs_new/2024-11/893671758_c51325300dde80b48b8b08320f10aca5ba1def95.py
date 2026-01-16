import os
import json
from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Optional
import httpx

app = FastAPI()

# Obtener la URL del servidor LLM desde una variable de entorno
LLM_SERVER_URL = os.environ.get("LLM_SERVER_URL", "http://localhost:11434")  

class Query(BaseModel):
    """
    Modelo para las solicitudes a la API.
    """
    prompt: str
    model: str = "mistral:7b"  # Cambiado el modelo por defecto a mistral
    stream: Optional[bool] = True

async def stream_generated_text(prompt: str, model: str = "mistral:7b"):
    """
    Genera texto de forma asíncrona usando Mistral.
    """
    url = f"{LLM_SERVER_URL}/api/generate"
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            # Configuración específica para Mistral
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": True,
                "options": {
                    "temperature": 0.7,
                    "top_p": 0.95,
                    "top_k": 40,
                }
            }
            
            async with client.stream("POST", url, json=payload) as response:
                if response.status_code != 200:
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"Error al conectar con el servidor LLM: {response.status_code} - {response.text}"
                    )

                async for chunk in response.aiter_bytes():
                    decoded_chunk = chunk.decode('utf-8')
                    for line in decoded_chunk.split("\n"):
                        if line.strip():
                            try:
                                data = json.loads(line)
                                if "response" in data:
                                    yield data["response"]
                            except json.JSONDecodeError:
                                continue

        except httpx.RequestError as e:
            raise HTTPException(status_code=500, detail=f"Error de comunicación con el servidor LLM: {e}")

async def get_generated_text(prompt: str, model: str = "mistral:7b"):
    """
    Obtiene el texto completo generado por Mistral.
    """
    url = f"{LLM_SERVER_URL}/api/generate"
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            # Configuración específica para Mistral
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.7,
                    "top_p": 0.95,
                    "top_k": 40,
                }
            }
            
            response = await client.post(url, json=payload)
            response.raise_for_status()

            combined_response = ""
            for line in response.text.splitlines():
                if line.strip():
                    try:
                        data = json.loads(line)
                        if "response" in data:
                            combined_response += data["response"]
                    except json.JSONDecodeError:
                        continue

            return {"response": combined_response}

        except httpx.RequestError as e:
            raise HTTPException(status_code=500, detail=f"Error de comunicación con el servidor LLM: {e}")

# Los endpoints permanecen iguales
@app.post("/api/generate")
async def generate_text(query: Query):
    if query.stream:
        return StreamingResponse(
            stream_generated_text(query.prompt, query.model),
            media_type="text/plain"
        )
    else:
        response = await get_generated_text(query.prompt, query.model)
        return JSONResponse(response)

@app.post("/api/models/download")
async def download_model(llm_name: str = Body(..., embed=True)):
    url = f"{LLM_SERVER_URL}/api/pull"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json={"name": llm_name})
            response.raise_for_status()
            return {"message": f"Model {llm_name} downloaded successfully"}
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Error al descargar el modelo: {e}")

@app.get("/api/models")
async def list_models():
    url = f"{LLM_SERVER_URL}/api/tags"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            return {"models": response.json()["models"]}
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener la lista de modelos: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3335)