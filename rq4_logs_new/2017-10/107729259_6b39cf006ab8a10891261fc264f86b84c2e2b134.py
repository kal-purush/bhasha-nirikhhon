
class Selectel:

    def __init__(self, core):
        self.core = core
        self.telegram = core.telegram.BOT
        self.sdk = core.selectel

        self.commands = {
            'auth': self.storage_auth,
            'containers': self.send_container_list,
            'select_container': self.select_container,
            'upload_file': self.upload_file,
            'download_file': self.download_file
        }

    def storage_auth(self, message):
        text = message.get('text')
        chat = message.get('chat')

        cmd, user, password = text.split(' ', 2)

        if not user or not password:
            self.telegram.send_message('Введите логин и пароль в формате /auth {login} {pass}', chat_id=chat['id'])

        self.sdk.storage_auth(chat['id'], user, password)

        self.send_container_list(message)

    def send_container_list(self, message):

        chat = message.get('chat')

        containers = self.sdk.get_containers_list()

        if len(containers) == 0:
            self.telegram.send_message('Доступных контейнеров нет', chat_id=chat['id'])
            return

        buttons = []

        for container in containers:
            name = container.get('name')

            row = [{
                'text': name,
                'callback_data': '{}|{}'.format('select_container', name)
            }]
            buttons.append(row)

        message = 'Выбирете контейнер, с которым хотите работать:'

        self.telegram.send_message(message, chat_id=chat['id'], reply_markup={'inline_keyboard': buttons})

    def select_container(self, callback_query):
        data = callback_query.get('data')
        message = callback_query.get('message')
        chat = message.get('chat')

        cmd, container = data.split('|', 1)

        files = self.sdk.get_files_list(container)

        self.core.states.push(chat['id'], {'type': 'container', 'container': container})
        buttons = []

        message = 'Для загрузки файла, прикрепите его в сообщении.'

        if len(files) is not 0:
            message += ' Для скачивания файла, выберите его из списка:'

            for file in files:
                name = file.get('name')

                row = [{
                    'text': name,
                    'callback_data': '{}|{}|{}'.format('download_file', container, name)
                }]
                buttons.append(row)

        self.telegram.send_message(message, chat_id=chat['id'], reply_markup={'inline_keyboard': buttons})

    def upload_file(self, message):
        document = message['document']
        chat = message['chat']

        tmp_name = self.telegram.telegram_return_file(document['file_id'])

        state = self.core.states.get(chat['id'])

        container = state.get('container')
        name = document.get('file_name')

        response = self.sdk.upload_file(container, name, open(tmp_name), document.get('mime_type'), document.get('file_size'))

        if response.status_code == 201:
            message = 'Файл успешно загружен'
        else:
            message = 'Ошибка при загрузке файла'

        self.telegram.send_message(message, chat['id'])

    def download_file(self, callback_query):
        data = callback_query.get('data')
        chat = callback_query['message']['chat']

        cmd, container, file = data.split('|', 2)

        content = self.sdk.download_file(container, file)

        self.telegram.send_document(content, chat['id'])

    def auth(self, chat_id):
        if not self.sdk.authorized:
            self.sdk.storage_auth(chat_id)

        if not self.sdk.authorized:
            return False

        return True

    def request_auth(self, chat_id):
        self.telegram.send_message('Пожалуйста, авторизуйтесь c помощью команы /auth', chat_id=chat_id)

    def process_command(self, cmd, message):

        document = message.get('document')
        chat = message.get('chat')

        if not self.auth(chat['id']) and cmd != 'auth':
            self.request_auth(chat['id'])
            return

        state = self.core.states.get(chat['id'])

        if document and state.get('type') == 'container':
            self.commands['upload_file'](message)
        else:
            self.commands[cmd](message)

    def process_callback_query(self, cmd, callback_query):

        chat = callback_query['message'].get('chat')

        if not self.auth(chat['id']):
            self.request_auth(chat['id'])
            return

        self.commands[cmd](callback_query)