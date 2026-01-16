import json
import os
import shutil
import sys

import arrow
from PySide.QtCore import QTimer
from PySide.QtGui import QApplication, QWidget, QHBoxLayout, QFileDialog, QListWidgetItem, QPushButton, QListWidget, \
    QInputDialog

FILENAME_FORMAT = 'YYYY-MM-DD HH-mm-ss'
AUTOSAVE_TIMEOUT = 5*60*1000

app = QApplication(sys.argv)

main_window = QWidget()
main_window.resize(640, 480)
main_window.setWindowTitle('Save manager')

box = QHBoxLayout()
main_window.setLayout(box)


def load_config():
    _config = {}

    if os.path.exists("config.json"):
        with open("config.json") as f:
            _config = json.loads(f.read())

    return _config


config = load_config()


def save_config():
    with open("config.json", 'w') as f:
        f.write(json.dumps(config))


def select_path_clicked():
    dialog = QFileDialog()
    path = dialog.getExistingDirectory()

    config['save_folder_path'] = path
    config['backup_folder_path'] = os.path.join(path, "backups")
    config['remote_folder_path'] = os.path.join(path, "remote")

    if not os.path.exists(config['backup_folder_path']):
        os.mkdir(config['backup_folder_path'])

    update_save_list()
    save_config()


def update_save_list():
    save_list.clear()
    try:
        file_list = os.listdir(config['backup_folder_path'])
        file_list.reverse()
        for filename in file_list:
            text = filename
            try:
                created = arrow.get(filename, FILENAME_FORMAT)
                created.shift(hour=-1)
                text = created.humanize(locale='en_gb')
            except Exception as e:
                print e

            list_item = QListWidgetItem()
            list_item.setData(1, filename)
            list_item.setText(text)
            save_list.addItem(list_item)

    except Exception as e:
        print "Error opening directory", e


select_path_button = QPushButton("Select path")
select_path_button.clicked.connect(select_path_clicked)
box.addWidget(select_path_button)

save_list = QListWidget()
box.addWidget(save_list)


def create_backup(filename=None):
    if not filename:
        timestamp = arrow.now().format(FILENAME_FORMAT)
        str(timestamp)

    remote_path = config['remote_folder_path']
    backup_path = os.path.join(config['backup_folder_path'], filename)
    shutil.copytree(remote_path, backup_path)
    update_save_list()


def backup_clicked():
    filename, ok = QInputDialog.getText(main_window, 'Input Dialog', 'Enter save name:')
    if ok:
        create_backup(filename)


backup_button = QPushButton("Back up now")
backup_button.clicked.connect(backup_clicked)
box.addWidget(backup_button)


def autosave():
    create_backup()

timer = QTimer()
timer.timeout.connect(create_backup)


def restore_clicked():
    if save_list.selectedItems():
        selected_item = save_list.selectedItems()[0]
        # print selected_item.text()
        filename = selected_item.data(1)

        shutil.rmtree(config['remote_folder_path'])
        shutil.copytree(
            os.path.join(config['backup_folder_path'], filename),
            config['remote_folder_path']
        )

restore_button = QPushButton("Restore selected")
restore_button.clicked.connect(restore_clicked)
box.addWidget(restore_button)


autosave_button = QPushButton("Autosave Start")

auto_save = False


def toggle_autosave():
    global auto_save
    if not auto_save:
        timer.start(AUTOSAVE_TIMEOUT)
        auto_save = True
        autosave_button.setText("Autosave Stop")
    else:
        timer.stop()
        auto_save = False
        autosave_button.setText("Autosave Start")

autosave_button.clicked.connect(toggle_autosave)
box.addWidget(autosave_button)

update_save_list()
main_window.show()

sys.exit(app.exec_())