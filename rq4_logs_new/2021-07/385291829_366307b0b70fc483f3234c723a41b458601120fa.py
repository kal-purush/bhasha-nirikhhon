from flask import Flask, request, abort, render_template, url_for, flash, redirect, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin, LoginManager, login_user, current_user, logout_user, login_required
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from datetime import datetime, timedelta
import json
import os
import ast
import time

try:
    from meta import SQLALCHEMY_DATABASE_URI, SECRET_KEY, DEBUG, channel_access_token, channel_secret, api_key1, api_secret1

except:
    SQLALCHEMY_DATABASE_URI = os.environ['DATABASE_URL']
    SECRET_KEY = os.environ['SECRET_KEY']
    DEBUG = False
    channel_access_token = os.environ['CHANNEL_ACCESS']
    channel_secret = os.environ['CHANNEL_SECRET']
    api_key1 = os.environ['api_key1']
    api_secret1 = os.environ['api_secret1']


from linebot import (
    LineBotApi, WebhookHandler, WebhookParser
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import *

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['SECRET_KEY'] = SECRET_KEY
app.config['DEBUG'] = DEBUG
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = 'login'
login_manager.login_message_category = 'info'

# Channel Access Token
line_bot_api = LineBotApi(channel_access_token)
# Channel Secret
handler = WebhookHandler(channel_secret)

parser = WebhookParser(channel_secret)


@login_manager.user_loader
def load_user(user_id):
    return None

class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username =  db.Column(db.String(20), unique=True, nullable=False)


class Recruits(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    #date.....
    line = db.Column(db.String)
    name = db.Column(db.String)
    highschool = db.Column(db.String)
    ## how did you hear about us?
    number = db.Column(db.String)
    dept = db.Column(db.String)
    questions = db.Column(db.String)
    set1 = db.Column(db.String)
    set2 = db.Column(db.String)
    set3 = db.Column(db.String)
    set4 = db.Column(db.String)
    set5 = db.Column(db.String)
    set6 = db.Column(db.String)
    set7 = db.Column(db.String)
    set8 = db.Column(db.String)
    status = db.Column(db.Integer)

class MyModelView(ModelView):
    def is_accessible(self):
        if DEBUG == True:
            return True
        else:
            return True

admin = Admin(app)
admin.add_view(MyModelView(User, db.session))
admin.add_view(MyModelView(Recruits, db.session))


@app.route("/login/<string:pw>", methods=['GET','POST'])
def login(pw):
    print('LOGIN')

    if pw == 'pw':
        user = User.query.filter_by(username='Admin').first()
        print(user.username)
        login_user(user)
        print(user, 'loggedin')
        return redirect (url_for('data'))
    else:
        return 'Cannot login'

''' UI pages '''

@app.route('/')
def home():
    return 'Hello, World!'

@app.route('/data')
@login_required
def data():
    return 'Data'



''' Line Bot Actions '''

@app.route('/text/<string:tx>')
def text(tx):
    try:
        line_bot_api.multicast(['U2dc560609e55883a4d869c88c0d912e7'], TextSendMessage(text=tx))
        # max 150 recipients
        # what if recipient has blocked or deleted bot??
    except LineBotApiError as e:
        abort(400)
    return tx

@app.route('/set/<string:tx>')
def text(tx):
    try:
        line_bot_api.multicast(['U2dc560609e55883a4d869c88c0d912e7', 'U2dc081c2670e8322666ca4d890df6562'], TextSendMessage(text=tx))
        # max 150 recipients
        # what if recipient has blocked or deleted bot??
    except LineBotApiError as e:
        abort(400)
    return tx


def message_list(arg, info):

    if arg == 'alert':
        message = TextSendMessage(text='Sorry this answer is too long, please make it shorter (<20)')
        return message

    if arg == 'nameSet':
        image = ImageSendMessage(
            original_content_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/partnership.png',
            preview_image_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/partnership.png'
        )
        message = TextSendMessage(text='Thanks, got it. Next, please write your highschool...')
        return [image, message]

    if arg == 'highSet':
        image = ImageSendMessage(
            original_content_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/phone.png',
            preview_image_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/phone.png'
        )
        message = TextSendMessage(text='Okay, thanks. Could we have your phone number for contacting?')
        return [image, message]

    if arg == 'numSet':
        image = ImageSendMessage(
            original_content_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/globe.png',
            preview_image_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/globe.png'
        )
        message = TextSendMessage(text='This is going well! Now which department are you interested in?')
        confirm = TemplateSendMessage(
            alt_text='Which department?',
            template=ButtonsTemplate(
                thumbnail_image_url= 'https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/logo2.PNG',
                title='Which department?',
                text='Jin Wen Applied Foreign Languages has two divsions.',
                actions=[
                    PostbackAction(
                        label='English Division',
                        display_text='English Division',
                        data="['Division', 'Eng']"
                    ),
                    PostbackAction(
                        label= 'Japanese Division',
                        display_text='Japanese Division',
                        data="['Division', 'Jpn']"
                    ),
                    PostbackAction(
                        label= 'Another Department',
                        display_text='Another Department',
                        data="['Division', 'Other']"
                    )
                ]
            )
        )
        return [image, message, confirm]

    if arg == 'deptSet':
        sticker = StickerSendMessage(package_id='2', sticker_id='141')
        message = TextSendMessage(text='Great, all set. Now the BOT can help with any questions you might have about the Department or our Application Process')
        return [sticker, message]


    if arg == "welcome":
        image = ImageSendMessage(
            original_content_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/logo1.PNG',
            preview_image_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/logo1.PNG'
        )
        message1 = TextSendMessage(text='Welcome to JinWen Applied Foreign Languages Department!')
        sticker = StickerSendMessage(package_id='2', sticker_id='144')
        message2 = TextSendMessage(text='How are you today?')
        return [image, message1, sticker, message2]

    if arg == 'start1':
        message1 = TextSendMessage(text='This BOT is here to help with any question you have about the Department or our application process')
        message2 = TextSendMessage(text='First we need some simple details....')
        return [message1, message2]

    if arg == 'start2':
        image = ImageSendMessage(
            original_content_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/human.png',
            preview_image_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/human.png'
        )
        message3 = TextSendMessage(text='Should we use your LINE name?')
        message4 = TemplateSendMessage(
            alt_text='Confirm template',
            template=ConfirmTemplate(
                text='Name',
                actions=[
                    PostbackAction(
                        label=info,
                        display_text='NAME SET: ' + info ,
                        data="['Name', '" + info + "']"
                    ),
                    PostbackAction(
                        label= 'Not this',
                        display_text='Name...',
                        data="['Name', '159']"
                    ),
                ]
            )
        )

        return [image, message3, message4]


    if arg == 'nameConfirm':
        message = TemplateSendMessage(
            alt_text='Confirm template',
            template=ConfirmTemplate(
                text='Check Name',
                actions=[
                    PostbackAction(
                        label=info,
                        display_text='NAME SET: ' + info ,
                        data="['Name', '" + info + "']"
                    ),
                    PostbackAction(
                        label= 'Not this',
                        display_text='Name..',
                        data="['Name', '159']"
                    ),
                ]
            )
        )
        return message

    if arg == 'highConfirm':
        print('HIGH MESSAGE')
        message = TemplateSendMessage(
            alt_text='Confirm template',
            template=ConfirmTemplate(
                text='Check Highschool',
                actions=[
                    PostbackAction(
                        label=info,
                        display_text='HIGHSCHOOL SET: ' + info ,
                        data="['High', '" + info + "']"
                    ),
                    PostbackAction(
                        label= 'Not this',
                        display_text='One more time.',
                        data="['High', None]"
                    ),
                ]
            )
        )
        return message

    if arg == 'numConfirm':
        print('NUMBER MESSAGE')
        message = TemplateSendMessage(
            alt_text='Buttons template',
            template=ConfirmTemplate(
                text='Check Number',
                actions=[
                        PostbackAction(
                            label=info,
                            display_text='NUMBER SET: ' + info,
                            data="['Num', '" + info + "']"
                        ),
                        PostbackAction(
                            label='Try again',
                            display_text="Okay, let's try again.'",
                            data="['Num', None]"
                        )
                    ]
                )
            )
        return message

    if arg == 'genConfirm':
        message = TemplateSendMessage(
            alt_text='Confirm template',
            template=ConfirmTemplate(
                text='Ask another question?',
                actions=[
                    PostbackAction(
                        label='Yes',
                        display_text='I want to learn more',
                        data="['Gen', 'Yes']"
                    ),
                    PostbackAction(
                        label= 'No',
                        display_text='I will learn more later',
                        data="['Gen', 'No']"
                    ),
                ]
            )
        )
        return message


    if arg == 'check':
        print('CHECK MESSAGE')
        message = TemplateSendMessage(
            alt_text='Buttons template',
            template=ButtonsTemplate(
                thumbnail_image_url= 'https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/globe.png',
                title='Thank you for getting in touch',
                text='Name:  Highschool:...',
                actions=[
                        PostbackAction(
                            label="All good :)",
                            display_text="Nice, now lets think more about your future studies",
                            data="Done"
                        ),
                        PostbackAction(
                            label="Start again",
                            display_text="No problem, from the top! 1) What is your name?",
                            data="Restart"
                        ),


                    ]
                )
            )
        return message


    if arg == 'general':
        message = TemplateSendMessage(
            alt_text='ImageCarousel template',
            template=ImageCarouselTemplate(
                columns=[
                    ImageCarouselColumn(
                        image_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/faculty.png',
                        action=PostbackAction(
                            label='Faculty',
                            display_text='I would like to know about JUST AFLD teachers...',
                            data="['Faculty', 'None']"
                        )
                    ),
                    ImageCarouselColumn(
                        image_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/application.png',
                        action=PostbackAction(
                            label='Application',
                            display_text='How can I apply for JUST AFLD?',
                            data="['Apply', 'None']"
                        )
                    ),
                    ImageCarouselColumn(
                        image_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/courses.png',
                        action=PostbackAction(
                            label='Courses',
                            display_text='What kind of course would I study at JUST AFLD?',
                            data="['Courses', 'None']"
                        )
                    ),
                    ImageCarouselColumn(
                        image_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/contact.png',
                        action=PostbackAction(
                            label='Contact',
                            display_text='Can I contact someone at JUST AFLD?',
                            data="['Contact', 'None']"
                        )
                    ),
                    ImageCarouselColumn(
                        image_url='https://lms-tester.s3-ap-northeast-1.amazonaws.com/line-bot/why.png',
                        action=PostbackAction(
                            label='Why JUST?',
                            display_text='Why is JUST AFLD a good choice for me?',
                            data="['Why', 'None']"
                        )
                    )

                ]
            )
        )
        return message




def rich_menu(userID):
    rich_menu_to_create = RichMenu(
    size=RichMenuSize(width=2500, height=800),
    selected=False,
    name="Nice richmenu",
    chat_bar_text="Tap here",
    areas=[
        RichMenuArea(
        bounds=RichMenuBounds(x=0, y=0, width=1250, height=800),
        action=URIAction(label='Go to line.me', uri='https://line.me')),
        RichMenuArea(
        bounds=RichMenuBounds(x=1250, y=0, width=1250, height=800),
        action=URIAction(label='Go to line.me', uri='https://github.com/line/demo-rich-menu-bot'))
        ]
    )
    rich_menu_id = line_bot_api.create_rich_menu(rich_menu=rich_menu_to_create)
    print('RMID ', rich_menu_id)

    with open('banner.png', 'rb') as f:
        line_bot_api.set_rich_menu_image(rich_menu_id, 'image/png', f)

    'https://medium.com/enjoy-life-enjoy-coding/%E4%BD%BF%E7%94%A8-python-%E7%82%BA-line-bot-%E5%BB%BA%E7%AB%8B%E7%8D%A8%E4%B8%80%E7%84%A1%E4%BA%8C%E7%9A%84%E5%9C%96%E6%96%87%E9%81%B8%E5%96%AE-rich-menus-7a5f7f40bd1'

    line_bot_api.link_rich_menu_to_user(userID, rich_menu_id)


def follow_check(events):
    newUser = events[0].source.user_id

    if events[0].type == 'follow':
        print('ID follow', newUser)
        newRec = Recruits(line=newUser, status=1)
        db.session.add(newRec)
        db.session.commit()

        try:
            line_bot_api.unlink_rich_menu_from_user(newUser)
        except:
            pass
        line_bot_api.push_message(newUser, message_list('welcome', None))
        return 'OK'

    if events[0].type == 'unfollow':
        print('ID unfollow', newUser)
        recruit = Recruits.query.filter_by(line=newUser).first()
        recruit.status = 0
        recruit.line = '0000__' + recruit.line
        db.session.commit()



@app.route("/callback", methods=['POST'])
def callback():
    print('CALLBACK')
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']
    print('SIGNATURE', signature)

    # get request body as text
    body = request.get_data(as_text=True)
    print('BODY', str(body))
    app.logger.info("Request body: " + body)

    # parse webhook body
    try:
        events = parser.parse(body, signature)
        follow_check(events)
    except InvalidSignatureError:
        abort(400)

    # handle webhook body
    try:
        print('HANDLER try')
        handler.handle(body, signature)
        print('HANDLER success')
    except InvalidSignatureError:
        abort(400)
    return 'OK'




@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):

    #message = TextSendMessage(text=event.message.text)
    userID = event.source.user_id
    recruit = Recruits.query.filter_by(line=userID).first()

    tx = event.message.text

    if recruit.status < 2:
        profile = line_bot_api.get_profile(userID)
        name = profile.display_name
        if recruit.name == None:
            message = message_list('start1', name)
            line_bot_api.push_message(userID, message)
            time.sleep(4)
            message = message_list('start2', name)
        elif recruit.name == '159':
            if len(tx) < 21:
                name = event.message.text
                message = message_list('nameConfirm', name)
            else:
                message = message_list('alert', None)

    elif recruit.status < 3:
        if len(tx) < 21:
            high = event.message.text
            message = message_list('highConfirm', high)
        else:
            message = message_list('alert', None)
    elif recruit.status < 4:
        if len(tx) < 21:
            number = event.message.text
            message = message_list('numConfirm', number)
        else:
            message = message_list('alert', None)
    else:
        message = message_list('general', None)


    print('EVENT', event)
    line_bot_api.reply_message(event.reply_token, message)



@handler.add(PostbackEvent)
def handle_message(event, destination):
    if isinstance(event.source, SourceUser):
        print('ID', event.source.user_id) # user_id replaces userId

    print('POSTBACK', event.postback.data)
    data = event.postback.data

    if data == 'None':
        pass
        return None
    else:
        userID = event.source.user_id
        recruit = Recruits.query.filter_by(line=userID).first()

    '''
    if data == 'Done':
        ## send message
        return 'OK'
    if data == 'Restart':
        recruit.delete()
        db.session.commit()
        newRec = Recruits(line=userID, status='follow')
        db.session.add(newRec)
        db.session.commit()
        return 'OK'
    '''

    def send(message):
        line_bot_api.reply_message(event.reply_token, message)

    def push(arg):
        if arg == 1:
            time.sleep(3)
            message = message_list('genConfirm', None)
            line_bot_api.push_message(userID, message)


    data_list = ast.literal_eval(event.postback.data)
    print('DATALIST', data_list)


    if data_list[0] == 'Name':
        if data_list[1] == '159':
            recruit.name = data_list[1]
            message = TextSendMessage(text='Please enter your name...')
            send(message)
        else:
            recruit.name = data_list[1]
            recruit.status = 2
            message = message_list('nameSet', None)
            send(message)

    if data_list[0] == 'High':
        if data_list[1] == None:
            message = TextSendMessage(text='Please enter your highschool...')
            send(message)
        else:
            recruit.highschool = data_list[1]
            recruit.status = 3
            message = message_list('highSet', None)
            send(message)
    if data_list[0] == 'Num':
        if data_list[1] == None:
            message = TextSendMessage(text='Please enter your phone number...')
            send(message)
        else:
            recruit.number = data_list[1]
            recruit.status = 4
            message = message_list('numSet', None)
            send(message)
    if data_list[0] == 'Division':
        recruit.dept = data_list[1]
        recruit.status = 5
        message = message_list('deptSet', None)
        line_bot_api.push_message(userID, message)
        time.sleep(2)
        message = message_list('general', None)
        send(message)
        #rich_menu(userID)


    if data_list[0] == 'Faculty':
        recruit.set1 = data_list[0]
        message = TextSendMessage(text='Faculty info coming soon...')
        send(message)
        push(1)
    if data_list[0] == 'Courses':
        recruit.set2 = data_list[0]
        message = TextSendMessage(text='Courses info coming soon...')
        send(message)
        push(1)
    if data_list[0] == 'Contact':
        recruit.set3 = data_list[0]
        message = TextSendMessage(text='Here is a list of professors you can add to your LINE....')
        send(message)
        push(1)
    if data_list[0] == 'Why':
        recruit.set4 = data_list[0]
        message = TextSendMessage(text='Why study langauges? Why study at university? Answers coming soon....')
        send(message)
        push(1)
    if data_list[0] == 'Apply':
        recruit.set5 = data_list[0]
        message = TextSendMessage(text='Our application process is very easy. First...')
        send(message)
        push(1)

    if data_list[0] == 'Gen':
        if data_list[1] =='Yes':
            message = message_list('general', None)
            send(message)
        else:
            m1 = TextSendMessage(text='Great, thank you for paying an interest in our department.')
            m2 = TextSendMessage(text='JUST-BOT will send some useful information in the future')
            m3 = TextSendMessage(text='Just say "Hi" to get more information')
            sticker = StickerSendMessage(package_id='2', sticker_id='159')

            send([m1, m2, sticker, m3])





    db.session.commit()
    return 'OK'





@handler.add(FollowEvent)
def handle_follow():
    print('student has joined')

@handler.add(UnfollowEvent)
def handle_unfollow():
    print('student has left')

#If there is no handler for an event, this default handler method is called.
@handler.default()
def default(event):
    print('DEFAULT', event)
    message_id = event.message.id
    message_content = line_bot_api.get_message_content(message_id)

    with open('/', 'wb') as fd:
        for chunk in message_content.iter_content():
            fd.write(chunk)




if __name__ == '__main__':
    app.run(debug=True)
