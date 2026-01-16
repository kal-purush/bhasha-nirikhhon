"""1. Задание на закрепление знаний по модулю CSV. Написать скрипт, осуществляющий выборку определенных данных из
файлов info_1.txt, info_2.txt, info_3.txt и формирующий новый «отчетный» файл в формате CSV. Для этого: Создать
функцию get_data(), в которой в цикле осуществляется перебор файлов с данными, их открытие и считывание данных. В
этой функции из считанных данных необходимо с помощью регулярных выражений извлечь значения параметров «Изготовитель
системы», «Название ОС», «Код продукта», «Тип системы». Значения каждого параметра поместить в соответствующий
список. Должно получиться четыре списка — например, os_prod_list, os_name_list, os_code_list, os_type_list. В этой же
функции создать главный список для хранения данных отчета — например, main_data — и поместить в него названия
столбцов отчета в виде списка: «Изготовитель системы», «Название ОС», «Код продукта», «Тип системы». Значения для
этих столбцов также оформить в виде списка и поместить в файл main_data (также для каждого файла); Создать функцию
write_to_csv(), в которую передавать ссылку на CSV-файл. В этой функции реализовать получение данных через вызов
функции get_data(), а также сохранение подготовленных данных в соответствующий CSV-файл; Проверить работу программы
через вызов функции write_to_csv(). """

import csv
import re


def get_data(files):
    os_prod_list = []
    os_name_list = []
    os_code_list = []
    os_type_list = []
    data_list = [os_prod_list, os_name_list, os_code_list, os_type_list]
    main_data = ['Изготовитель системы', 'Название ОС', 'Код продукта', 'Тип системы']
    for file in files:
        with open(file) as f_n:
            f_n_reader = csv.reader(f_n)
            for row in f_n_reader:
                for i in range(4):
                    data = re.findall(rf'{main_data[i]}: +(.+)', row[0])
                    if data:
                        data_list[i].append(data[0])
    main_list = [main_data]
    for i in range(len(os_prod_list)):
        main_list.append([])
        for v in range(4):
            main_list[i + 1].append(data_list[v][i])
    return main_list


def write_to_csv(write_to_file, read_from_files):
    data = get_data(read_from_files)
    with open(write_to_file, 'w') as f_n:
        f_n_writer = csv.writer(f_n)
        for row in data:
            f_n_writer.writerow(row)


if __name__ == '__main__':
    write_to_csv('main_data.csv', ['info_1.txt', 'info_2.txt', 'info_3.txt'])
