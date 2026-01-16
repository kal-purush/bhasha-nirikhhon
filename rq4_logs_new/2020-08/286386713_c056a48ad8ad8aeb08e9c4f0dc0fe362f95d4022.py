from PySide2.QtWidgets import QApplication, QMainWindow, QToolBar, \
     QStatusBar, QAction, QCommonStyle, QDockWidget, QCalendarWidget, QWidget, QListView
from PySide2.QtGui import QIcon
from PySide2.QtCore import Qt
'''
pip install pyside2
'''
app = QApplication()
mainwindow = QMainWindow()
# 设置界面最小尺寸
mainwindow.setMinimumSize(900, 600)

# 创建工具栏对象
tool_bar = QToolBar()

open_dir_action = QAction(QIcon(QCommonStyle().standardPixmap(QCommonStyle.SP_DirIcon)), '打开目录')
tool_bar.addAction(open_dir_action)
open_file_action = QAction(QIcon(QCommonStyle().standardPixmap(QCommonStyle.SP_FileIcon)), '新建文件')
tool_bar.addAction(open_file_action)
open_delete_action = QAction(QIcon(QCommonStyle().standardPixmap(QCommonStyle.SP_TrashIcon)), '删除')
tool_bar.addAction(open_delete_action)

dock_calendar_widget = QDockWidget()
dock_calendar_widget.setWidget(QCalendarWidget())
dock_listview_widget = QDockWidget()
dock_listview_widget.setWidget(QListView())

# 添加在dock widgets区域的右边，Qt是一个包含了各种常量的包
mainwindow.addDockWidget(Qt.RightDockWidgetArea, dock_calendar_widget)
mainwindow.addDockWidget(Qt.RightDockWidgetArea, dock_listview_widget)

# 添加一个空的widget
mainwindow.setCentralWidget(QWidget())

# 创建状态栏对象
statusbar = QStatusBar()
statusbar.showMessage('我是statusbar')

# 添加工具栏
mainwindow.addToolBar(tool_bar)
# 添加状态栏
mainwindow.setStatusBar(statusbar)
mainwindow.show()
app.exec_()