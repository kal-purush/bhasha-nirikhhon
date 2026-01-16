import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QMessageBox


class MyMainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Simple PyQt5 Application")
        self.setGeometry(100, 100, 400, 200)  # Set window position and size

        self.init_ui()

    def init_ui(self):
        # Create a button
        button = QPushButton("Click me!", self)
        button.setGeometry(150, 80, 100, 40)  # Set button position and size

        # Connect button click event to a function
        button.clicked.connect(self.show_message_box)

    def show_message_box(self):
        # Display a message box when the button is clicked
        QMessageBox.information(self, "Message", "Button clicked!")


if __name__ == "__main__":
    app = QApplication(sys.argv)

    main_window = MyMainWindow()
    main_window.show()

    sys.exit(app.exec_())
