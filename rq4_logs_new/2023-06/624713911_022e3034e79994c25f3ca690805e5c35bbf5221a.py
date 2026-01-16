import email
import os
import poplib
import re
from email.header import decode_header


class POP3_Client:
    def __init__(self, server, port, username, password):
        self.server = server
        self.port = port
        self.username = username
        self.password = password

        self.read_last_message()  # чтение последнего сообщения из почтового ящика
        if self.message:
            self.data_processing()  # разбор и вывод разобранных данных письма

    def read_last_message(self):
        pop_server = poplib.POP3_SSL(self.server, self.port)  # подключение к серверу POP3 по SSL
        pop_server.user(self.username)  # аутентификация пользователя
        pop_server.pass_(self.password)

        num_messages = len(pop_server.list()[1])  # получение количества сообщений в почтовом ящике

        if num_messages < 1:  # если нет сообщений в почтовом ящике
            print("No messages in mailbox.")
            return None

        raw_email = pop_server.retr(num_messages)[1]  # получение последнего сообщения
        raw_email = b'\n'.join(raw_email)
        self.message = email.message_from_bytes(raw_email)  # преобразование в объект email.message.Message
        pop_server.quit()  # завершение сеанса POP3

    def data_processing(self):
        subject = decode_header(self.message.get('Subject'))[0]  # получение заголовка темы письма
        from_ = decode_header(self.message.get('From'))[0]  # получение отправителя письма
        date = decode_header(self.message.get('Date'))[0]  # получение даты письма

        data = {
            'subject': subject[0].decode(subject[1]) if subject[1] else subject[0],
            'from': from_[0].decode(from_[1]) if from_[1] else from_[0],
            'date': date[0].decode(date[1]) if date[1] else date[0]
        }

        if self.message.is_multipart():  # проходимся по всем частям сообщения
            for part in self.message.walk():
                content_type = part.get_content_type()
                if content_type == "text/plain":  # если это обычный текст, то извлекаем и декодируем
                    body = part.get_payload(decode=True)
                    data['body'] = body.decode()
                elif content_type == "application/octet-stream":  # если вложение, извлекаем имя файла из заголовка
                    file_name = decode_header(part.get("Content-Disposition"))[0][0].split(';')[1].split('=')[1]
                    if file_name:  # создаем каталог для вложений (если он не существует) и сохраните вложение в файл
                        os.makedirs('attachments', exist_ok=True)
                        filepath = os.path.join('attachments', file_name).replace("\"", "")
                        with open(filepath, 'wb') as f:
                            f.write(part.get_payload(decode=True))
        else:
            data['body'] = self.message.get_payload(decode=True)

        data['body'] = re.sub(
            '[\n\t  ]{2,}',
            '\n',
            data['body']
        )  # замена нескольких последовательных символов пробела одним символом новой строки

        print(
            f"Тема: {data['subject']}\n"
            f"'От кого: {data['from']}\n"
            f"Дата: {data['date']}\n"
            f"Содержимое: {data['body']}"
        )  # вывод содержимого


if __name__ == "__main__":
    print('введите логин(почту): ')
    username = input()
    print('введите пароль: ')
    password = input()
    client = POP3_Client('pop.yandex.com', 995, username, password)