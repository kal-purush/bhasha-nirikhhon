from flask import Flask, request, abort

from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
)

app = Flask(__name__)

line_bot_api = LineBotApi('VrinLe+lNFxs8L1VJJbU+NLPVUx3OrnTaOq7t1InIAQ6IohbBJiUGlGR1v4X7X9Rko61a5TWt51XIxaP9HBZOWhS2Pe5mPafYLqeJYLvjDq93dK3DH2D6SbxpIYQOPIye4/yVg6Z/vjsMegj2ImIPAdB04t89/1O/w1cDnyilFU=')
handler = WebhookHandler('de482eb516e18a7f8e5f72f198bcda26')


@app.route("/callback", methods=['POST'])
def callback():
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)

    # handle webhook body
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        print("Invalid signature. Please check your channel access token/channel secret.")
        abort(400)

    return 'OK'


@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=event.message.text))


if __name__ == "__main__":
    app.run()