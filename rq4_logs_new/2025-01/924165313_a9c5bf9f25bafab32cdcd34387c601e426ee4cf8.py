from fastapi import FastAPI, Request
import requests
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

app = FastAPI()

# Token de verificação do Webhook
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "meu_token_secreto")

# Credenciais do WhatsApp API
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID")
WHATSAPP_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")

@app.get("/")
async def root():
    return {"message": "API do Webhook do WhatsApp ativa!"}

@app.get("/webhook")
async def verify_webhook(hub_mode: str = None, hub_verify_token: str = None, hub_challenge: str = None):
    """
    Verifica a solicitação do webhook para garantir que veio do WhatsApp.
    """
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        return int(hub_challenge)  # WhatsApp espera um número como resposta
    return {"error": "Token inválido"}, 403

@app.post("/webhook")
async def receive_whatsapp_webhook(request: Request):
    """
    Processa mensagens recebidas do WhatsApp e responde automaticamente.
    """
    data = await request.json()

    if "entry" in data:
        for entry in data["entry"]:
            for change in entry.get("changes", []):
                if "messages" in change.get("value", {}):
                    for message in change["value"]["messages"]:
                        user_id = message["from"]

                        # Mensagem automática de resposta
                        response_text = (
                            "Olá! 🎉 Obrigado por entrar em contato. "
                            "Estamos aqui para te ajudar! 😊\n\n"
                            "Se precisar de suporte, envie a palavra *AJUDA*."
                        )

                        # Enviar resposta via WhatsApp
                        send_whatsapp_message(user_id, response_text)

    return {"status": "Mensagem processada"}

def send_whatsapp_message(to: str, message: str):
    """
    Envia mensagem via WhatsApp Cloud API.
    """
    url = f"https://graph.facebook.com/v20.0/{WHATSAPP_PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": message}
    }
    response = requests.post(url, json=payload, headers=headers)
    return response.json()