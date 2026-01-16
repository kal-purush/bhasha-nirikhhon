from mainwindow import Ui_MainWindow
from model import TaskList
from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot, QModelIndex


class Controller():
    def __init__(self):
        self._model = TaskList()

    text_changed = pyqtSignal(str)


    @pyqtSlot(QModelIndex, name='on_listView_clicked')
    def on_listview_clicked(self, index):
        task_text = ""
        if index.isValid():
            task_text = self._model.get_task(index).text
        self.text_changed.emit(task_text)





