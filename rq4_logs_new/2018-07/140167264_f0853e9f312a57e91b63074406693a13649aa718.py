# coding: UTF-8

# Задача-1:
# Напишите скрипт, создающий директории dir_1 - dir_9 в папке,
# из которой запущен данный скрипт.
# И второй скрипт, удаляющий эти папки.

import os
import shutil
import sys

def make_dir(name):
    os.mkdir(name)


def remove_dir(name):
    os.rmdir(name)

def dirs():
    list_of_dirs = [i for i in os.listdir() if os.path.isdir(i)]
    return list_of_dirs

if __name__ == "__main__":

    for i in range(1, 10):
        dir_name = "dir_" + str(i)
        make_dir(dir_name)
        print("Директория ", dir_name, " создана")
        i += 1

    for i in range(1, 10):
        dir_name = "dir_" + str(i)
        remove_dir(dir_name)
        print("Директория ", dir_name, " удалена!")
        i +=1

    # Задача-2:
    # Напишите скрипт, отображающий папки текущей директории.

    print(dirs())

    # Задача-3:
    # Напишите скрипт, создающий копию файла, из которого запущен данный скрипт.

    path_to_file = sys.argv[0]

    list_of_path = path_to_file.split("/")

    file_name = list_of_path[-1]
    new_file = file_name + '_copy'

    shutil.copy(file_name, new_file)

    if os.path.exists(new_file):
        print("Файл ", file_name, " был скопирован")
    else:
        print('Возникли проблемы при копировании!')

