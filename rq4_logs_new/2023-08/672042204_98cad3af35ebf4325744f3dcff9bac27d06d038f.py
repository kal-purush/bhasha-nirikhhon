import os
import requests

def main():
    bot_token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_TO"]
    message_thread_id = os.environ["TELEGRAM_THREAD_ID"]
    github_server_url = os.environ["GITHUB_SERVER_URL"]
    github_repository = os.environ["GITHUB_REPOSITORY"]

    pr_url = f"{github_server_url}/{github_repository}/pull"
    message = f"Nueva Pull Request abierta: {pr_url}"

    send_message(bot_token, chat_id, message)

def send_message(bot_token, chat_id, text):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "message_thread_id" : message_thread_id,
        "parse_mode": "HTML"
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        print("Mensaje enviado con éxito")
    else:
        print("Error al enviar el mensaje:", response.text)

if __name__ == "__main__":
    main()