import sys
import asyncio
import asyncqt
import sqlite3
import requests
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QVBoxLayout, QHBoxLayout,
    QTableView, QLineEdit, QPushButton, QWidget, QMessageBox,
    QProgressBar, QStatusBar, QDialog, QFormLayout, QDialogButtonBox
)
from PyQt5.QtGui import QStandardItemModel, QStandardItem


class MainApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Многозадачное приложение")
        self.resize(800, 600)
        self.db_connection = sqlite3.connect("posts.db")
        self.create_table_if_not_exists()  # Создать таблицу, если её нет
        self.init_ui()

        # Настройка таймера для периодических обновлений
        self.timer = QTimer()
        self.timer.timeout.connect(self.check_updates)
        self.timer.start(10000)  # Проверка каждые 10 секунд

    def create_table_if_not_exists(self):
        cursor = self.db_connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                title TEXT,
                body TEXT
            )
        """)
        self.db_connection.commit()

    def init_ui(self):
        # Основной виджет
        central_widget = QWidget(self)
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # Поле поиска
        self.search_field = QLineEdit(self)
        self.search_field.setPlaceholderText("Поиск по заголовку...")
        layout.addWidget(self.search_field)

        # Таблица
        self.table_view = QTableView(self)
        layout.addWidget(self.table_view)

        # Модель данных
        self.model = QStandardItemModel(self)
        self.setup_table_model()

        # Кнопки
        button_layout = QHBoxLayout()
        self.load_button = QPushButton("Загрузить данные", self)
        self.load_button.clicked.connect(self.load_data)

        self.add_button = QPushButton("Добавить запись", self)
        self.add_button.clicked.connect(self.add_record)

        self.delete_button = QPushButton("Удалить запись", self)
        self.delete_button.clicked.connect(self.delete_record)

        button_layout.addWidget(self.load_button)
        button_layout.addWidget(self.add_button)
        button_layout.addWidget(self.delete_button)
        layout.addLayout(button_layout)

        # Статус-бар
        self.status_bar = QStatusBar(self)
        self.setStatusBar(self.status_bar)

        # Изначальное сообщение в статус-баре
        self.status_bar.showMessage("Приложение запущено", 5000)  # Сообщение исчезнет через 5 секунд

        # Прогресс бар
        self.progress_bar = QProgressBar(self)
        self.progress_bar.setAlignment(Qt.AlignCenter)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

    def setup_table_model(self):
        self.model.setHorizontalHeaderLabels(["ID", "User ID", "Title", "Body"])
        self.table_view.setModel(self.model)
        self.load_table_data()

    def load_table_data(self):
        self.model.removeRows(0, self.model.rowCount())  # Очистка таблицы
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT * FROM posts")
        for row in cursor.fetchall():
            items = [QStandardItem(str(field)) for field in row]
            self.model.appendRow(items)

    def filter_table(self):
        search_text = self.search_field.text().lower()
        for row in range(self.model.rowCount()):
            title_item = self.model.item(row, 2)
            if search_text in title_item.text().lower():
                self.table_view.setRowHidden(row, False)
            else:
                self.table_view.setRowHidden(row, True)

    async def fetch_data(self):
        url = "https://jsonplaceholder.typicode.com/posts"
        self.status_bar.showMessage("Загрузка данных...")
        self.progress_bar.setValue(20)  # Установка значения прогресс бара
        await asyncio.sleep(2)  # Имитация задержки
        response = requests.get(url)
        if response.status_code == 200:
            self.progress_bar.setValue(50)  # Установка значения прогресс бара
            data = response.json()
            return data
        else:
            QMessageBox.warning(self, "Ошибка", "Не удалось загрузить данные")
            return []

    async def save_data_to_db(self, data):
        self.status_bar.showMessage("Сохранение данных...")
        self.progress_bar.setValue(70)  # Установка значения прогресс бара
        await asyncio.sleep(2)  # Имитация задержки
        cursor = self.db_connection.cursor()

        for item in data:
            user_id = item.get('userId')  # Изменено на 'userId', так как API возвращает 'userId', а не 'user_id'
            title = item.get('title')
            body = item.get('body')

            if user_id is not None:  # Проверяем, что user_id существует
                # Вставляем запись, даже если она уже существует
                cursor.execute("INSERT INTO posts (user_id, title, body) VALUES (?, ?, ?)",
                               (user_id, title, body))

        self.db_connection.commit()
        self.progress_bar.setValue(100)  # Установка значения прогресс бара

    async def load_data_task(self):
        data = await self.fetch_data()
        if data:
            await self.save_data_to_db(data)
            self.load_table_data()
            self.status_bar.showMessage("Данные загружены и сохранены.", 5000)
            self.progress_bar.setValue(0)  # Сброс значения прогресс бара

    def load_data(self):
        asyncio.create_task(self.load_data_task())

    def add_record(self):
        dialog = AddRecordDialog(self)
        if dialog.exec_():  # Проверяем, была ли нажата кнопка "OK"
            user_id = dialog.user_id_input.text()
            title = dialog.title_input.text()
            body = dialog.body_input.text()

            if user_id and title and body:  # Проверяем, что все поля заполнены
                cursor = self.db_connection.cursor()
                cursor.execute("INSERT INTO posts (user_id, title, body) VALUES (?, ?, ?)",
                               (user_id, title, body))
                self.db_connection.commit()  # Сохраняем изменения
                self.load_table_data()  # Обновляем таблицу
            else:
                QMessageBox.warning(self, "Ошибка", "Все поля должны быть заполнены.")

    def delete_record(self):
        selected_indexes = self.table_view.selectionModel().selectedRows()
        if not selected_indexes:
            QMessageBox.warning(self, "Удаление записи", "Выберите запись для удаления.")
            return

        confirm = QMessageBox.question(self, "Удаление записи", "Удалить выбранную запись?",
                                       QMessageBox.Yes | QMessageBox.No)
        if confirm == QMessageBox.Yes:
            row = selected_indexes[0].row()
            record_id = int(self.model.item(row, 0).text())
            cursor = self.db_connection.cursor()
            cursor.execute("DELETE FROM posts WHERE id = ?", (record_id,))
            self.db_connection.commit()
            self.load_table_data()
            QMessageBox.information(self, "Удаление записи", "Запись успешно удалена.")

    def check_updates(self):
        asyncio.create_task(self.fetch_updates())

    async def fetch_updates(self):
        # Имитация проверки обновлений
        await asyncio.sleep(1)
        self.status_bar.showMessage("Проверка обновлений завершена.", 5000)

    def closeEvent(self, event):
        self.db_connection.close()
        super().closeEvent(event)


class AddRecordDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Добавить запись")
        self.layout = QFormLayout(self)

        self.user_id_input = QLineEdit(self)
        self.title_input = QLineEdit(self)
        self.body_input = QLineEdit(self)

        self.layout.addRow("User ID:", self.user_id_input)
        self.layout.addRow("Title:", self.title_input)
        self.layout.addRow("Body:", self.body_input)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, self)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)

        self.layout.addWidget(self.buttons)
        self.setLayout(self.layout)


if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Настройка интеграции asyncio и PyQt
    loop = asyncqt.QEventLoop(app)
    asyncio.set_event_loop(loop)

    # Запуск приложения
    with loop:
        main_app = MainApp()
        main_app.show()
        loop.run_forever()