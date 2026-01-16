import json
from channels.generic.websocket import WebsocketConsumer
from asgiref.sync import async_to_sync
from datetime import datetime
from .models import User, Message, ChatName
import re


class ChatConsumer(WebsocketConsumer):
    def connect(self):
        url_chat_hash_user = self.scope['path'][23:-1]
        try:
            self.room_group_name = ChatName.objects.get(user_first__hash=url_chat_hash_user,
                                                        user_second=self.scope['user'])
        except:
            self.room_group_name = ChatName.objects.get(user_first=self.scope['user'],
                                                        user_second__hash=url_chat_hash_user).name
        self.room_group_name = re.sub("@", ".", str(self.room_group_name))
        async_to_sync(self.channel_layer.group_add)(
            self.room_group_name,
            self.channel_name
        )
        self.accept()
        self.send(text_data=json.dumps({
            'type': 'connection_established',
            'message': 'You are now connected!!',
        }))

    def receive(self, text_data):
        text_data_json = json.loads(text_data)
        message = text_data_json['message']
        to_id = User.objects.get(hash=text_data_json['users'][6:-1])
        Message.objects.create(from_id=self.scope["user"], to_id=to_id, message=message)

        async_to_sync(self.channel_layer.group_send)(
            self.room_group_name,
            {
                'type': 'chat_message',
                'message': message,
                'user_which_send': self.scope["user"]
            }
        )

    def chat_message(self, event):
        message = event['message']
        now = datetime.now()
        self.send(text_data=json.dumps({
            'type': 'chat',
            'user': User.objects.get(username=event['user_which_send']).username,
            'message': message,
            'time_now': now.strftime("%I:%M") if now.strftime("%I:%M")[0] != '0' else now.strftime("%I:%M")[1:],
            'avatar': User.objects.get(username=event['user_which_send']).avatar.url
        }))