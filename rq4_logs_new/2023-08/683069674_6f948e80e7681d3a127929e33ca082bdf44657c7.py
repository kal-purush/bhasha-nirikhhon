# requirements製作 pip freeze > requirements.txt
# 在heroku上建立資料表 heroku run flask db upgrade,接著到pgAdmin中Register一個server (輸入Heroku PostgreSQL),然後到connection
# 接著要去連線,然後找到
from flask import Flask, request, abort
#可以將字串轉換成字典 action = service&category = 按摩調理>>{'action':'service','category':'按摩調理'}
from urllib.parse import parse_qsl

from events.basic import *
from events.service import *
from line_bot_api import *
from events.admin import *
from extensions import db, migrate
from models.user import User
import os
app = Flask(__name__)
#讓程式自己去判斷如果是測試端就會使用APP_SETTINGS
app.config.from_object(os.environ.get('APP_SETTINGS', 'config.DevConfig'))
# app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://admin:Meng0614@localhost:5432/mspa'
# app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.app = app
db.init_app(app)
migrate.init_app(app, db)


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

    message_text = str(event.message.text).lower()
    user = User.query.filter(User.line_id == event.source.user_id).first()#取得user的第一筆資料
    #如果沒有user的資料時,才會透過api去取得
    if not user:
        profile = line_bot_api.get_profile(event.source.user_id)#line API中說明get_profile可以取得的資料
        print(profile.display_name)
        print(profile.user_id)#相同的好友會因為不同的profile而有不同的user_id
        print(profile.picture_url)

        user = User(profile.user_id, profile.display_name, profile.picture_url)
        db.session.add(user)
        db.session.commit()

    
    print(user.id)
    print(user.line_id)
    print(user.display_name)

    if message_text == '@關於我們':
        about_us_event(event)

    elif message_text == '@營業據點':
        location_event(event)

    elif message_text == '@預約服務':
        service_category_event(event)
    




@handler.add(FollowEvent)
def handle_follow(event):
    welcome_msg = """Hello! 您好，歡迎您成為寶石服飾的好友！

我是小幫手 

-想預約服務都可以直接跟我互動喔~
-直接點選下方【資訊】選單功能

-期待您的光臨！"""

    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=welcome_msg))


@handler.add(UnfollowEvent)
def handle_unfollow(event):
    print(event)


if __name__ == "__main__":
    app.run()