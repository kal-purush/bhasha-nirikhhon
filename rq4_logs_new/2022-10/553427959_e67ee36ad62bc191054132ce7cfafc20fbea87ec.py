import requests
from settings import BOT_TOKEN, DEVELOPER_CHAT_ID, ALL_CHAT_ID


def send_msg_all(text_list: tuple):
    """
    Производит отправку в TG канал всем пользователям из списка настроек хоть раз запустивших бота
    """
    for text in text_list:
        for chat_id in ALL_CHAT_ID:
            url_req = "https://api.telegram.org/bot" + BOT_TOKEN + "/sendMessage" + \
                      "?chat_id=" + chat_id + "&text=" + text + "&disable_web_page_preview=True"
            requests.get(url_req)
    
    
def send_msg_developer(text):
    """
    Производит отправку в TG канал разработчику с ID из настроек
    """
    url_req = "https://api.telegram.org/bot" + BOT_TOKEN + "/sendMessage" + \
              "?chat_id=" + DEVELOPER_CHAT_ID + "&text=" + text
    requests.get(url_req)