import pickle
from PyQt5 import QtCore


class Task:

    def __init__(self):
        self.id = id
        self.name = ""
        self.task_text = ""
        self.complete = False


class TaskList(QtCore.QAbstractListModel):

    def __init__(self, parent = None):
        QtCore.QAbstractListModel.__init__(self, parent)
        self.__file_name = "Tasks.dat"
        self.__task_list = []

    def rowCount(self, parent):
        return len(self.__task_list)

    def data(self, index, role):
        if role == QtCore.Qt.DisplayRole | role == QtCore.Qt.EditRole:
            return self.__task_list[index].name
        if role == QtCore.Qt.CheckStateRole:
            return self.__task_list[index].complete

    def setData(self, index, value, role):
        if role == QtCore.Qt.DisplayRole | role == QtCore.Qt.EditRole:
            self.__task_list[index].name = value
        elif role == QtCore.Qt.CheckStateRole:
            self.__task_list[index].complete = value

    def removeRow(self, task_index, parent):
        super().beginRemoveRows(parent, task_index, task_index)
        del self.__task_list[task_index]
        super().endRemoveRows()

    def insertRow(self, task, parent=QtCore.QModelIndex()):
        task.id = len(self.__task_list) + 1
        super().beginInsertRows(parent, len(self.__task_list), len(self.__task_list))
        self.__task_list.append(task)
        super().endInsertRows()

    def load(self):
        try:
            with open(self.__file_name, 'rb') as f:
                self.__task_list = pickle.load(f)
        except:
            self.__task_list = []

    def save(self):
        with open(self.__file_name, 'rb') as f:
            pickle.dump(self.__task_list, f)
