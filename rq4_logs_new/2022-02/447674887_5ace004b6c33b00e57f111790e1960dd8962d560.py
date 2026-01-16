from PyQt5 import uic
from PyQt5.QtGui import QStandardItemModel, QStandardItem
from PyQt5.QtWidgets import QWidget, qApp


class AdminConsole(QWidget):
    '''
    класс графичееского иннтерфеса сервера
    '''

    def __init__(self, parent=None):
        super().__init__()
        # Использование функции loadUi()
        uic.loadUi('gui_server.ui', self)  # загружаем наше окно

        # Обрабокта события нажатия кнопки
        self.actionExit.triggered.connect(qApp.quit)


    def users_list(self, database):
        """
        создает контент таблицы со всеми зарегистрированными пользователями
        :param database: база, с которой надо работать
        :param online: отображать всех или только онлайн
        :return: таблица пользователей
        """
        # создаем модель таблицы
        users_table = QStandardItemModel()

        # проверяем онлайн или нет
        if self.checkBox.isChecked():
            # обозначаем заголовки
            users_table.setHorizontalHeaderLabels(['Username', 'Last login', 'IP', 'Port'])
            # забираем userlist из бд
            user_list = database.get_online_user_list()
            # создаем ячейку из каждой строки
            for row in user_list:
                user, id, time, port, ip = row
                user = QStandardItem(user)  # создаем элемент
                user.setEditable(False)  # редактирование
                time = QStandardItem(str(time.replace(microsecond=0)))
                time.setEditable(False)
                ip = QStandardItem(ip)  # создаем элемент
                ip.setEditable(False)  # редактирование
                port = QStandardItem(port)  # создаем элемент
                port.setEditable(False)  # редактирование
                users_table.appendRow([user, time, ip, port])  # добавляем строку
        else:
            # обозначаем заголовки
            users_table.setHorizontalHeaderLabels(['Username', 'Last login'])
            # забираем userlist из бд
            user_list = database.get_user_list()
            # создаем ячейку из каждой строки
            for row in user_list:
                user, time = row
                user = QStandardItem(user)  # создаем элемент
                user.setEditable(False)  # редактирование
                time = QStandardItem(str(time.replace(microsecond=0)))
                time.setEditable(False)
                users_table.appendRow([user, time])  # добавляем строку
        return users_table

    def login_history_list(self, database):
        """
        создаем таблицу с историй подключения к серверу
        :param database: используемая база данных
        :return: таблицу с историей
        """
        login_history_list = database.get_login_history_list()
        login_history_table = QStandardItemModel()
        login_history_table.setHorizontalHeaderLabels(['Username', 'Last login', 'IP', "Port"])
        for row in login_history_list:
            user, id, time, ip, port = row
            user = QStandardItem(user)  # создаем элемент
            user.setEditable(False)  # редактирование
            ip = QStandardItem(ip)
            ip.setEditable(False)
            port = QStandardItem(str(port))
            port.setEditable(False)
            # Уберём милисекунды из строки времени, т.к. такая точность не требуется.
            time = QStandardItem(str(time.replace(microsecond=0)))
            time.setEditable(False)
            login_history_table.appendRow([user, time, ip, port])  # добавляем строку
        return login_history_table

    def logs_list(self):
        """
        передаем логи в listview
        :return: логи или ошибку
        """
        logs_listview = QStandardItemModel()
        # пробуем открыть файл с логами
        try:
            with open ('log/server.log') as logs_file:
                # построчно заполняем логи в модель
                for line in logs_file:
                    row = QStandardItem(line)
                    logs_listview.appendRow(row)
                return logs_listview
        # если ошибка, то выводим ее вместо логов
        except Exception as e:
            row = QStandardItem(str(e))
            logs_listview.appendRow(row)
            return logs_listview