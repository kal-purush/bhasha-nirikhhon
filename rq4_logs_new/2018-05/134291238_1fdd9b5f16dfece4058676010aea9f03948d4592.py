import sys
from PyQt5.QtPrintSupport import QPrintDialog
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtCore import *


class Ui(QMainWindow, QWidget):

    state = True

    def __init__(self, parent=None):
        QMainWindow.__init__(self, parent)
        self.frame()
        self.filename = ""
        self.show()

    def text_editor(self):
        self.text = QTextEdit()
        self.setCentralWidget(self.text)

        self.text.setTabStopWidth(33)
        self.text.cursorPositionChanged.connect(self.cursor_pos)
        # self.text.setWordWrapMode()
        #self.text.setWordWrapMode(False)

        self.undo_act.triggered.connect(self.text.undo)
        self.redo_act.triggered.connect(self.text.redo)
        self.cut_act.triggered.connect(self.text.cut)
        self.copy_act.triggered.connect(self.text.copy)
        self.paste_act.triggered.connect(self.text.paste)
        self.select_all_act.triggered.connect(self.text.selectAll)
        self.delete_act.triggered.connect(self.text.clear)

    def frame(self):
        self.setGeometry(200, 100, 800, 500)
        self.setWindowIcon(QIcon("icons/icons8-edit.png"))
        self.setWindowTitle("Text Editor")

        self.menu_bar()
        self.toolbar()
        self.status_bar()
        self.text_editor()
        self.format_bar()

    def word_wrap(self):
        if self.state:
            self.state = False
            self.text.setWordWrapMode(False)
        if not self.state:
            self.state = True
            self.text.setWordWrapMode(True)

    def new_file(self):
        spawn = Ui(self)
        spawn.show()

    def save_file(self):
        if not self.filename:
            self.filename = QFileDialog.getSaveFileName(self, "QFileDialog.getSaveFileName()",  "", "All Files (*);;Text Files (*.txt)")
        if not self.filename.endswith('.txt'):
            self.filename += ".txt"

    def open_file(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        fileName, _ = QFileDialog.getSaveFileName(self, "QFileDialog.getSaveFileName()", "",
                                                  "All Files (*);;Text Files (*.txt)", options=options)
        if fileName:
            with open(self.fileName, "r") as file:
                self.text.setText(file.read())

    def print(self):
        dialog = QPrintDialog()

        if dialog.exec_() == QDialog.Accepted:
            self.text.document().print_(dialog.printer())

    def font_set(self):
        font, ok = QFontDialog.getFont()
        if ok:
            self.text.setFont(font)

    def font_family(self, font):
        self.text.setFont(font)

    def font_size(self, font_size):
        self.text.setFontPointSize(int(font_size))

    def font_color(self):
        color = QColorDialog.getColor()
        self.text.setTextColor(color)

    def back_color(self):
        self.text.setAutoFillBackground(True)
        p = self.text.palette()
        p.setColor(self.text.backgroundRole(), Qt.white)
        self.text.setPalette(p)

    def bullet_list(self):
        cursor = self.text.textCursor()
        cursor.insertList(QTextListFormat.ListDisc)

    def numbered_list(self):
        cursor = self.text.textCursor()
        cursor.insertList(QTextListFormat.ListDecimal)

    def cursor_pos(self):
        cursor = self.text.textCursor()
        line = cursor.blockNumber()
        col = cursor.columnNumber()
        self.status.showMessage(str("Line: {} Col: {}".format(line, col)))

    def bold(self):

        if self.text.fontWeight() == QFont.Bold:

            self.text.setFontWeight(QFont.Normal)

        else:

            self.text.setFontWeight(QFont.Bold)

    def italic(self):

        state = self.text.fontItalic()

        self.text.setFontItalic(not state)

    def underline(self):

        state = self.text.fontUnderline()

        self.text.setFontUnderline(not state)

    def strike(self):

        fmt = self.text.currentCharFormat()
        fmt.setFontStrikeOut(not fmt.fontStrikeOut())
        self.text.setCurrentCharFormat(fmt)

    def superscript(self):

        fmt = self.text.currentCharFormat()
        align = fmt.verticalAlignment()

        if align == QTextCharFormat.AlignNormal:
            fmt.setVerticalAlignment(QTextCharFormat.AlignSuperScript)

        else:
            fmt.setVerticalAlignment(QTextCharFormat.AlignNormal)

        self.text.setCurrentCharFormat(fmt)

    def subscript(self):

        fmt = self.text.currentCharFormat()
        align = fmt.verticalAlignment()

        if align == QTextCharFormat.AlignNormal:
            fmt.setVerticalAlignment(QTextCharFormat.AlignSubScript)
        else:
            fmt.setVerticalAlignment(QTextCharFormat.AlignNormal)

        self.text.setCurrentCharFormat(fmt)

    def left_align(self):
        self.text.setAlignment(Qt.AlignLeft)

    def right_align(self):
        self.text.setAlignment(Qt.AlignRight)

    def center_align(self):
        self.text.setAlignment(Qt.AlignCenter)

    def justify_align(self):
        self.text.setAlignment(Qt.AlignJustify)

    def toggle_toolbar(self):
        state = self.tool.isVisible()
        self.tool.setVisiblity(not state)

    def toggle_status_bar(self):
        state = self.status.isVisible()
        self.status.setVisiblity(not state)

    def toggle_format_bar(self):
        state = self.format_bar.isVisible()
        self.format_bar.setVisiblity(not state)

    def insert_image(self):
        filename = QFileDialog.getOpenFileNames(self, 'insert image', ".", "Images (*.png *.xpm *.jpg *.bmp *.gif)")
        image = QImage(filename)
        if image.isNull():

            popup = QMessageBox(QMessageBox.Critical, "Image load error", "Could not load image file!", QMessageBox.Ok, self)
            popup.show()

        else:

            cursor = self.text.textCursor()

            cursor.insertImage(image, filename)

    def menu_bar(self):
        self.menu = self.menuBar()
        menu1 = self.menu.addMenu('File')

        self.new_act = QAction(QIcon("icons/icons8-edit-file.png"), 'New', self)
        self.new_act.setShortcut('Ctrl+N')
        self.new_act.setStatusTip('New Document')
        self.new_act.triggered.connect(self.new_file)

        self.save_act = QAction(QIcon("icons/icons8-save-button.png"), 'Save', self)
        self.save_act.setStatusTip('Save Document')
        self.save_act.setShortcut('Ctrl+S')
        self.save_act.triggered.connect(self.save_file)

        self.open_act = QAction(QIcon("icons/icons8-open.png"), '&Open', self)
        self.open_act.setShortcut('Ctrl+O')
        self.open_act.setStatusTip('Open Document')
        self.open_act.triggered.connect(self.open_file)

        self.print_act = QAction(QIcon("icons/icons8-print.png"), 'Print', self)
        self.print_act.setStatusTip('Print the file')
        self.print_act.setShortcut('Ctrl+P')
        self.print_act.triggered.connect(self.print)

        self.exit_act = QAction(QIcon("icons/icons8-exit-button.png"), 'Exit', self)
        self.exit_act.setShortcut('Alt+F4')
        self.exit_act.setStatusTip('Exit')
        self.exit_act.triggered.connect(QApplication.instance().quit)

        self.imp_act =QAction(QIcon("icons/icons8-import.png"), 'Import', self)
        self.imp_act.setStatusTip('Import File')
        # self.imp_act.triggered.connect()

        menu1.addAction(self.new_act)
        menu1.addAction(self.save_act)
        menu1.addAction(self.open_act)
        menu1.addAction(self.imp_act)
        menu1.addAction(self.print_act)
        menu1.addAction(self.exit_act)

        menu2 = self.menu.addMenu('Edit')

        self.undo_act = QAction(QIcon("icons/icons8-undo.png"), 'Undo', self)
        self.undo_act.setStatusTip('Undo')
        self.undo_act.setShortcut('Ctrl+Z')
        # self.undo_act.triggered.connect(self.text.undo())

        self.redo_act = QAction(QIcon("icons/icons8-redo.png"), 'Redo', self)
        self.redo_act.setShortcut('Ctrl+Y')
        self.redo_act.setStatusTip('Redo')
        # self.redo_act.triggered.connect(self.text.redo)

        self.cut_act = QAction(QIcon("icons/icons8-cut.png"), 'Cut', self)
        self.cut_act.setStatusTip('Cut Selected Text')
        self.cut_act.setShortcut('Ctrl+X')
        # self.cut_act.triggered.connect(self.text.cut)

        self.copy_act = QAction(QIcon("icons/icons8-copy.png"), 'Copy', self)
        self.copy_act.setShortcut('Ctrl+C')
        self.copy_act.setStatusTip('Copy Selected Text')
        # self.copy_act.triggered.connect(self.text.copy)

        self.paste_act = QAction(QIcon("icons/icons8-paste.png"), 'Paste', self)
        self.paste_act.setStatusTip('Paste')
        self.paste_act.setShortcut('Ctrl+V')
        # self.paste_act.triggered.connect(self.text.paste)

        self.delete_act = QAction(QIcon("icons/icons8-trash-can.png"), 'Delete', self)
        self.delete_act.setShortcut('Del')
        self.delete_act.setStatusTip('Delete File')
        # self.delete_act.triggered.connect(self.text.)

        self.find_act = QAction(QIcon("icons/icons8-find.png"), 'Find', self)
        self.find_act.setStatusTip('Find a Word')
        self.find_act.setShortcut('Ctrl+F')

        self.select_all_act = QAction(QIcon("icons/icons8-select-all.png"), 'Select all', self)
        self.select_all_act.setShortcut('Ctrl+A')
        self.select_all_act.setStatusTip('Select all Text')
        # self.select_all_act.triggered.connect(self.text.selectAll())

        menu2.addAction(self.undo_act)
        menu2.addAction(self.redo_act)
        menu2.addAction(self.cut_act)
        menu2.addAction(self.copy_act)
        menu2.addAction(self.paste_act)
        menu2.addAction(self.delete_act)
        menu2.addAction(self.find_act)
        menu2.addAction(self.select_all_act)

        menu3 = self.menu.addMenu('Format')

        self.bullet_act = QAction(QIcon("icons/icons8-bulleted-list"), 'Bulleted List', self)
        self.bullet_act.setStatusTip('Insert Bulleted List')
        self.bullet_act.triggered.connect(self.bullet_list)

        self.numbered_act = QAction(QIcon("icons/icons8-numbered-list"), 'Numbered List', self)
        self.numbered_act.setStatusTip('Insert Numbered List')
        self.numbered_act.triggered.connect(self.numbered_list)

        menu3.addAction(self.bullet_act)
        menu3.addAction(self.numbered_act)
        # menu3.addAction(self.font_color_act)
        # menu3.addAction(self.back_color_act)

        menu4 = self.menu.addMenu('View')

        self.font_act = QAction(QIcon("icons/icons8-choose-font.png"), 'Font', self)
        self.font_act.setStatusTip('Customize the fonts')
        self.font_act.triggered.connect(self.font_set)

        self.word_act = QAction(QIcon(), 'Word wrap', self)
        self.word_act.triggered.connect(self.word_wrap)

        self.toolbar_act = QAction("Toggle Toolbar", self)
        self.toolbar_act.triggered.connect(self.toggle_toolbar)

        self.formatbar_act = QAction("Toggle Formatbar", self)
        self.formatbar_act.triggered.connect(self.toggle_format_bar)

        self.statusbar_act = QAction("Toggle Statusbar", self)
        self.statusbar_act.triggered.connect(self.toggle_status_bar)

        self.image_act = QAction(QIcon(), 'Inset Image', self)
        self.image_act.setStatusTip("Insert Image")
        self.image_act.triggered.connect(self.insert_image)

        menu4.addAction(self.word_act)
        menu4.addAction(self.font_act)
        menu4.addAction(self.toolbar_act)
        menu4.addAction(self.formatbar_act)
        menu4.addAction(self.statusbar_act)

        menu5 = self.menu.addMenu('Help')

    def toolbar(self):
        self.tool = self.addToolBar('Options')

        self.tool.addAction(self.new_act)
        self.tool.addAction(self.open_act)
        self.tool.addAction(self.save_act)

        self.tool.addSeparator()

        self.tool.addAction(self.cut_act)
        self.tool.addAction(self.copy_act)
        self.tool.addAction(self.paste_act)
        self.tool.addAction(self.redo_act)
        self.tool.addAction(self.undo_act)

        self.tool.addSeparator()

        self.tool.addAction(self.find_act)
        self.tool.addAction(self.image_act)

        self.tool.addSeparator()

        self.tool.addAction(self.bullet_act)
        self.tool.addAction(self.numbered_act)

        self.tool.addSeparator()

        self.addToolBarBreak()

    def status_bar(self):
        self.status = self.statusBar()

    def format_bar(self):
        self.format_bar = self.addToolBar('Format')

        font_box_act = QFontComboBox(self)
        font_box_act.currentFontChanged.connect(self.font_family)

        font_size_act = QComboBox(self)
        font_size_act.setEditable(True)
        font_size_act.setMinimumContentsLength(3)
        font_size_act.activated.connect(self.font_size)

        font_sizes = ['1', '2', '4', '5', '6', '8', '10', '12', '14', '16', '18', '20', '24', '28', '32', '38', '42']

        for i in font_sizes:
            font_size_act.addItem(i)

        self.font_color_act = QAction(QIcon("icons/icons8-text-color.png"), 'Font Colour', self)
        self.font_color_act.triggered.connect(self.font_color)

        self.back_color_act = QAction(QIcon("icons/icons8-color-palette.png"), 'Background Colour', self)
        self.back_color_act.triggered.connect(self.back_color)

        bold_act = QAction(QIcon("icons/icons8-bold.png"), 'Bold Font', self)
        bold_act.triggered.connect(self.bold)

        italic_act = QAction(QIcon("icons/icons8-italics.png"), 'Italic Font', self)
        italic_act.triggered.connect(self.italic)

        underline_act = QAction(QIcon("icons/icons8-underline.png"), 'Underlined Text', self)
        underline_act.triggered.connect(self.underline)

        super_act = QAction(QIcon("icons/icons8-superscript.png"), 'Superscript', self)
        super_act.triggered.connect(self.superscript)

        strike_act = QAction(QIcon("icons/icons8-strike through.png"), 'StrikeOut Text', self)
        strike_act.triggered.connect(self.strike)

        sub_act = QAction(QIcon("icons/icons8-subscript.png"), 'Subscript', self)
        sub_act.triggered.connect(self.subscript)

        align_left_act = QAction(QIcon("icons/icons8-text-align-left.png"), 'Align Text In Left', self)
        align_left_act.triggered.connect(self.left_align)

        align_right_act = QAction(QIcon("icons/icons8-align-right.png"), 'Align Text In Right', self)
        align_right_act.triggered.connect(self.right_align)

        align_center_act = QAction(QIcon("icons/icons8-align-center.png"), 'Align Text In Center', self)
        align_center_act.triggered.connect(self.center_align)

        align_justify_act = QAction(QIcon("icons/icons8-align-justify.png"), 'Align Text As Justify', self)
        align_justify_act.triggered.connect(self.justify_align)

        self.format_bar.addWidget(font_box_act)
        self.format_bar.addWidget(font_size_act)

        self.format_bar.addSeparator()

        self.format_bar.addAction(self.font_color_act)
        self.format_bar.addAction(self.back_color_act)

        self.format_bar.addSeparator()

        self.format_bar.addAction(bold_act)
        self.format_bar.addAction(italic_act)
        self.format_bar.addAction(underline_act)
        self.format_bar.addAction(strike_act)
        self.format_bar.addAction(super_act)
        self.format_bar.addAction(sub_act)

        self.format_bar.addSeparator()

        self.format_bar.addAction(align_left_act)
        self.format_bar.addAction(align_right_act)
        self.format_bar.addAction(align_center_act)
        self.format_bar.addAction(align_justify_act)

        self.format_bar.addSeparator()

        self.addToolBarBreak()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    call = Ui()
    sys.exit(app.exec_())