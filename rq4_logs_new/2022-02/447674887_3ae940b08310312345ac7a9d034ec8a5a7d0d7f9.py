import datetime

from sqlalchemy import __version__, create_engine, Table, Column, MetaData, Integer, String, Boolean, DateTime, \
    ForeignKey
from sqlalchemy.orm import mapper, sessionmaker


class MessengerStorage:
    print(f"Версия SQLAlchemy: {__version__}")

    class AllUsers:
        """
        класс для таблицы со всеми пользователями
        """

        def __init__(self, username):
            self.id = None
            self.username = username
            self.last_login = datetime.datetime.utcnow()

    class OnlineUsers():
        """
        класс для таблицы с онлайн пользователями.
        """

        def __init__(self, user_id, datetime, ip, port):
            self.user_id = user_id
            self.last_login = datetime
            self.ip = ip
            self.port = port

    class LoginHistory(OnlineUsers):
        """
        класс для таблицы с историей последнего подключения к серверу
        """

        def __init__(self, user_id, datetime, ip, port):
            super().__init__(user_id, datetime, ip, port)

    def __init__(self):
        self.engine = create_engine('sqlite:///messenger_db.sqlite', echo=False)
        self.metadata = MetaData()
        # таблица со всеми пользователями
        self.user_table = Table('Users', self.metadata,
                                Column('id', Integer, primary_key=True),
                                Column('username', String),
                                Column('last_login', DateTime))
        # таблица со пользователями онлайн
        self.online_user_table = Table('Online_users', self.metadata,
                                       Column('id', Integer, primary_key=True),
                                       Column('user_id', ForeignKey('Users.id'), unique=True),
                                       Column('last_login', DateTime),
                                       Column('port', Integer),
                                       Column('ip', String))
        # таблица со историями подключений
        self.login_history_table = Table('Login_history', self.metadata,
                                         Column('id', Integer, primary_key=True),
                                         Column('user_id', ForeignKey('Users.id')),
                                         Column('last_login', DateTime),
                                         Column('port', Integer),
                                         Column('ip', String))
        # создание таблиц
        self.metadata.create_all(self.engine)
        # связываем классы с таблицами
        mapper(self.AllUsers, self.user_table)
        mapper(self.OnlineUsers, self.online_user_table)
        mapper(self.LoginHistory, self.login_history_table)
        # создаем сессию
        server_session = sessionmaker(bind=self.engine)
        self.session = server_session()
        # обнуляем таблицу с активными юзерами при запуске сервера и сохраняем
        self.session.query(self.OnlineUsers).delete()
        self.session.commit()
    # геттеры
    def get_user_list(self):
        """
        геттер списка всех пользователей
        :return: список (username, last_login)
        """
        query = self.session.query(
            self.AllUsers.username,
            self.AllUsers.last_login
        )
        return query.all()

    def get_online_user_list(self):
        """
        геттер списка онлайн пользователей
        :return: список (username, last_login, ip, port)
        """
        query = self.session.query(
            self.AllUsers.username,
            self.OnlineUsers.user_id,
            self.OnlineUsers.last_login,
            self.OnlineUsers.ip,
            self.OnlineUsers.port
        ).join(self.AllUsers)
        return query.all()

    def get_login_history_list(self):
        """
        геттер истории подключений
        :return: список (username, last_login, ip, port)
        """
        query = self.session.query(
            self.AllUsers.username,
            self.LoginHistory.user_id,
            self.LoginHistory.last_login,
            self.LoginHistory.ip,
            self.LoginHistory.port
        ).join(self.AllUsers)
        return query.all()

    def login(self, username, ip, port):
        """
        обновление таблиц при подключении пользователя
        :param username: имя
        :param ip: ip
        :param port: порт
        :return: -
        """

        # пробуем достать юзера
        login_user = self.session.query(self.AllUsers).filter_by(username=username).first()
        # если юзер подключается впервые создаем запись в таблицу всех изеров
        if login_user is None:
            login_user = self.AllUsers(username)
            self.session.add(login_user)
            self.session.commit()
        # если юзер подключается не впервые - обновляем дату входа
        else:
            login_user.last_login = datetime.datetime.utcnow()
            self.session.commit()

        # добавляем в таблицу юзеров онлайн
        new_online_user = self.OnlineUsers(login_user.id, datetime.datetime.utcnow(), ip, port)
        self.session.add(new_online_user)
        # добавляем в таблицу историй подключений
        new_login_history_item = self.LoginHistory(login_user.id, datetime.datetime.utcnow(), ip, port)
        self.session.add(new_login_history_item)

        # сохраняем изменения
        self.session.commit()

    def logout(self, username):
        """
        удаляем пользователя из таблицы онлайн пользователей при отключении
        :param username: имя пользователя
        :return: -
        """
        user = self.session.query(self.AllUsers).filter_by(username=username).first()
        self.session.query(self.OnlineUsers).filter_by(user_id=user.id).delete()
        # сохраняем изменения
        self.session.commit()


# Отладка
if __name__ == '__main__':
    test_db = MessengerStorage()
    # выполняем 'подключение' пользователя
    test_db.login('client_1', '192.168.1.4', 8888)
    test_db.login('client_2', '192.168.1.5', 7777)
    # выводим список кортежей - активных пользователей
    print(test_db.get_online_user_list())
    # выполянем 'отключение' пользователя
    test_db.logout('client_1')

    # выводим список активных пользователей
    print(test_db.get_online_user_list())
    # запрашиваем историю входов по пользователю
    test_db.get_login_history_list()
    # выводим список известных пользователей
    print(test_db.get_user_list())