from image_handler import ImageHandler
from modifications_provider import ModificationsProvider
from main_view import *


def main():
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)

    image_handler = ImageHandler()
    image_handler.subscribe_view(ui)
    modifications_provider = ModificationsProvider()
    modifications_provider.subscribe_image_handler(image_handler)
    ui.subscribe_image_handler(image_handler)
    ui.subscribe_modifications_provider(modifications_provider)

    MainWindow.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()