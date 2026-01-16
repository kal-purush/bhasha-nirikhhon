import sys
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QLabel, QPushButton,
    QMessageBox, QGridLayout, QLineEdit, QFormLayout, QMainWindow, QStackedWidget
)
from PyQt5.QtGui import QPixmap, QFont, QIcon
from PyQt5.QtCore import Qt
import requests
from datetime import datetime


class LoginWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Cinema Hall POS - Login')
        self.setGeometry(100, 100, 400, 300)

        self.setStyleSheet("""
            QWidget {
                background-color: #2c3e50;
                color: white;
                font-family: Arial, sans-serif;
            }
            QLabel {
                color: #ecf0f1;
            }
            QLineEdit {
                background-color: #34495e;
                color: white;
                border: 1px solid #ecf0f1;
                padding: 5px;
            }
            QPushButton {
                background-color: #2980b9;
                color: white;
                border: none;
                padding: 10px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #3498db;
            }
        """)

        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        self.login_screen()

    def login_screen(self):
        self.header_label = QLabel('Cinema Hall POS')
        self.header_label.setFont(QFont('Arial', 24))
        self.header_label.setAlignment(Qt.AlignCenter)
        self.main_layout.addWidget(self.header_label)

        self.logo_label = QLabel(self)
        pixmap = QPixmap('path/to/logo.png')  # Replace with the path to your logo image
        self.logo_label.setPixmap(pixmap.scaled(100, 100, Qt.KeepAspectRatio))
        self.logo_label.setAlignment(Qt.AlignCenter)
        self.main_layout.addWidget(self.logo_label)

        form_layout = QFormLayout()

        self.username_input = QLineEdit()
        self.username_input.setFont(QFont('Arial', 14))
        form_layout.addRow('Username:', self.username_input)

        self.password_input = QLineEdit()
        self.password_input.setFont(QFont('Arial', 14))
        self.password_input.setEchoMode(QLineEdit.Password)
        form_layout.addRow('Password:', self.password_input)

        self.main_layout.addLayout(form_layout)

        self.login_button = QPushButton('Login')
        self.login_button.setFont(QFont('Arial', 14))
        self.login_button.clicked.connect(self.handle_login)
        self.main_layout.addWidget(self.login_button)

    def handle_login(self):
        username = self.username_input.text()
        password = self.password_input.text()

        response = requests.post('http://127.0.0.1:5000/login', json={'username': username, 'password': password})

        if response.status_code == 200:
            self.user_id = response.json()['user_id']
            self.open_main_menu()
        else:
            QMessageBox.warning(self, 'Error', 'Invalid credentials.')

    def open_main_menu(self):
        self.main_menu = MainMenuWindow(self.user_id)
        self.main_menu.show()
        self.close()


class MainMenuWindow(QMainWindow):
    def __init__(self, user_id):
        super().__init__()
        self.user_id = user_id
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Cinema Hall POS - Main Menu')
        self.setGeometry(100, 100, 1000, 800)

        self.setStyleSheet("""
            QWidget {
                background-color: #2c3e50;
                color: white;
                font-family: Arial, sans-serif;
            }
            QLabel {
                color: #ecf0f1;
            }
            QPushButton {
                background-color: #2980b9;
                color: white;
                border: none;
                padding: 10px;
                border-radius: 5px;
                margin: 10px;
            }
            QPushButton:hover {
                background-color: #3498db;
            }
        """)

        self.main_layout = QVBoxLayout()
        self.central_widget = QWidget()
        self.central_widget.setLayout(self.main_layout)
        self.setCentralWidget(self.central_widget)

        self.header_label = QLabel('Cinema Hall POS - Main Menu')
        self.header_label.setFont(QFont('Arial', 24))
        self.header_label.setAlignment(Qt.AlignCenter)
        self.main_layout.addWidget(self.header_label)

        self.now_showing_button = QPushButton('Now Showing')
        self.now_showing_button.setFont(QFont('Arial', 14))
        self.now_showing_button.clicked.connect(self.show_now_showing)
        self.main_layout.addWidget(self.now_showing_button)

        self.coming_soon_button = QPushButton('Coming Soon')
        self.coming_soon_button.setFont(QFont('Arial', 14))
        self.coming_soon_button.clicked.connect(self.show_coming_soon)
        self.main_layout.addWidget(self.coming_soon_button)

    def show_now_showing(self):
        self.clear_layout(self.main_layout)
        self.header_label = QLabel('Now Showing')
        self.header_label.setFont(QFont('Arial', 24))
        self.header_label.setAlignment(Qt.AlignCenter)
        self.main_layout.addWidget(self.header_label)

        self.movie_grid = QGridLayout()
        self.main_layout.addLayout(self.movie_grid)

        self.load_movies('now_showing')

    def show_coming_soon(self):
        self.clear_layout(self.main_layout)
        self.header_label = QLabel('Coming Soon')
        self.header_label.setFont(QFont('Arial', 24))
        self.header_label.setAlignment(Qt.AlignCenter)
        self.main_layout.addWidget(self.header_label)

        self.movie_grid = QGridLayout()
        self.main_layout.addLayout(self.movie_grid)

        self.load_movies('coming_soon')

    def load_movies(self, category):
        response = requests.get('http://127.0.0.1:5000/movies')

        if response.status_code == 200:
            movies = response.json()
            self.populate_movies(movies, category)
        else:
            QMessageBox.warning(self, 'Error', 'Failed to load movies.')

    def populate_movies(self, movies, category):
        self.clear_layout(self.movie_grid)

        count = 0
        row = 0
        col = 0

        for movie in movies:
            movie_release_date = datetime.strptime(movie['release_year'], '%Y')
            current_date = datetime.now()

            movie_widget = self.create_movie_widget(movie)

            if category == 'now_showing' and movie_release_date.year <= current_date.year:
                self.movie_grid.addWidget(movie_widget, row, col)
                count += 1
                col += 1
                if col > 2:
                    col = 0
                    row += 1
            elif category == 'coming_soon' and movie_release_date.year > current_date.year:
                self.movie_grid.addWidget(movie_widget, row, col)
                count += 1
                col += 1
                if col > 2:
                    col = 0
                    row += 1

    def create_movie_widget(self, movie):
        widget = QWidget()
        layout = QVBoxLayout()

        poster_label = QLabel()
        poster_pixmap = QPixmap()
        poster_pixmap.loadFromData(requests.get(movie['poster_url']).content)
        poster_label.setPixmap(poster_pixmap.scaledToWidth(200))

        title_label = QLabel(movie['title'])
        title_label.setFont(QFont('Arial', 14))
        title_label.setAlignment(Qt.AlignCenter)

        genre_label = QLabel(f"Genre: {movie['genre']}")
        genre_label.setFont(QFont('Arial', 12))
        genre_label.setAlignment(Qt.AlignCenter)

        synopsis_label = QLabel(movie['description'])
        synopsis_label.setFont(QFont('Arial', 10))
        synopsis_label.setAlignment(Qt.AlignCenter)
        synopsis_label.setWordWrap(True)

        layout.addWidget(poster_label)
        layout.addWidget(title_label)
        layout.addWidget(genre_label)
        layout.addWidget(synopsis_label)

        widget.setLayout(layout)
        return widget

    def clear_layout(self, layout):
        if layout is not None:
            while layout.count():
                child = layout.takeAt(0)
                if child.widget() is not None:
                    child.widget().deleteLater()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    login_window = LoginWindow()
    login_window.show()
    sys.exit(app.exec_())