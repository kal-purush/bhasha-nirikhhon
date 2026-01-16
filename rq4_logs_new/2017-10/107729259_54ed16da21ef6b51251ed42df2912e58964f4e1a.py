import uuid


class Notifier:

    def __init__(self, core):
        self.core = core
        self.table = self.core.db['notify_chats']

        self.set_routes()

    def set_routes(self):
        chats = self.get_all_chats()
        for chat in chats:
            uri = self.generate_uri(chat['uri'])
            self.core.server.hooks.add(uri, self.process_notify, chat)

    def get_all_chats(self):
        return list(self.table.find())

    async def process_notify(self, request, chat):
        try:
            post = await request.post()

            message = post['message']
            chat_id = chat['id']
            service = chat['service']

            if service == 'telegram':
                send_message = self.core.telegram.BOT.send_message
            # ...

            send_message(
                text=message,
                chat_id=chat_id
            )

            return {'text': 'ok'}

        except Exception as e:
            self.core.logger.error(e, exc_info=e)
            return {'text': 'Send POST request with "message" param', 'status': '500'}

    def process_start(self, chat_id, service):
        try:
            chat = self.table.find_one({'id': chat_id, 'service': service})
            if not chat:
                chat = self.save_chat_to_db(chat_id, service)

            link = "https://" +  self.core.PARAMS.get('host') + self.generate_uri(chat['uri'])

            message = "Вы можете присылать уведомления в чат от имени бота, оправив POST-запрос по адресу {} с текстом сообщения в параметре message.".format(link)

            if service == 'telegram':
                send_message = self.core.telegram.BOT.send_message
            # ...

            send_message(
                text=message,
                chat_id=chat_id
            )

        except Exception as e:
            self.core.logger.error(e, exc_info=e)

        return {'text': 'ok'}

    def save_chat_to_db(self, chat_id, service):
        key = self.generate_key()

        chat_model = {
            'id': chat_id,
            'service': service,
            'uri': key
        }

        self.core.server.hooks.add(self.generate_uri(key), self.process_notify, chat_model)

        self.table.insert_one(chat_model)

        return self.table.find_one(chat_model)

    """
    Generate uri by key

    :return string: uri
    """
    def generate_uri(self, key):
        return '/notify{}'.format(key)

    """
    Generate random key in uppercase

    :return string: key
    """
    def generate_key(self):
        return uuid.uuid4().hex[:8].upper()