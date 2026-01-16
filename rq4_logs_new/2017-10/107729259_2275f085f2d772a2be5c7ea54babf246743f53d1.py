class Main:

    def __init__(self, core):
        self.core = core
        self.telegram = core.telegram.BOT

        self.commands = {
            'start': self.start_command,
            'help': self.help_command
        }

    def start_command(self, message):
        chat = message.get('chat')

        message = "Привет! На связи ваш незаменимый помощник.\n" \
                  "Нажми /help, чтобы узнать, на что я способен."

        self.telegram.send_message(
            text=message,
            chat_id=chat['id']
        )

        return {'text': 'ok'}

    def help_command(self, message):
        chat = message.get('chat')

        message = "Доступные приложения:\n" \
                  "\n" \
                  "/notifier\t\tполучи вебхук для отправки сообщений в этот чат\n" \
                  "/cloudstorage\t\tмодуль для работы с облачным хранилищем Selectel"

        self.telegram.send_message(
            text=message,
            chat_id=chat['id']
        )

        return {'text': 'ok'}