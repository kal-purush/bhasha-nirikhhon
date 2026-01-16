"""
Задача 38:  Дополнить телефонный справочник возможностью изменения и удаления данных. Пользователь также может ввести
имя или фамилию, и Вы должны реализовать функционал для изменения и удаления данных.
"""

from os.path import exists
from csv import DictReader, DictWriter

def get_info():
    info = []
    first_name = input("Введите Фамилию: ")
    last_name = input("Введите Имя: ")
    info.append(first_name)
    info.append(last_name)
    flag = False
    while not flag:
        try:
            phone_number = int(input('Введите номер телефона: '))
            if len(str(phone_number)) != 11:
                print('wrong number')
            else:
                flag = True
        except ValueError:
            print('not valid number')
    info.append(phone_number)
    return info

def create_file():
    with open('phone.csv', 'w', encoding='utf-8') as data:
        # data.write('Фамилия;Имя;Номер\n')
        f_n_writer = DictWriter(data, fieldnames=['Фамилия', 'Имя', 'Номер'])
        f_n_writer.writeheader()

def write_file(lst):
    with open('phone.csv', 'a', encoding='utf-8') as data:
        data.write(f'{lst[0]},{lst[1]},{lst[2]}\n')
    print("Новый Абонент внесен в телефонный справочник")


def rerite(ind):
    with open('phone.csv', 'r+', encoding='utf-8', newline='') as f_n:
        f_n_reader = DictReader(f_n)
        res = list(f_n_reader)
        del res[ind - 1]
        # print(res)
    with open('phone.csv', 'w', encoding='utf-8', newline='') as f_n:
        f_n_writer = DictWriter(f_n, fieldnames=['Фамилия', 'Имя', 'Номер'])
        f_n.write('Фамилия,Имя,Номер\n')
        for el in res:
            f_n_writer.writerow(el)
        print("Абонент удален из телефонного справочника")

def read_file(file_name):
    # with open(file_name, encoding='utf-8') as data:
    #     phone_book = data.readlines()
    with open(file_name, encoding='utf-8') as f_n:
        f_n_reader = DictReader(f_n)
        phone_book = list(f_n_reader)
    return phone_book

def record_info():
    lst = get_info()
    write_file(lst)

def delete_entry():
    first_name = input("Вы желаете удалить свою запись, тогда наберите свою фамилию: ")
    with open('phone.csv', 'r+', encoding='utf-8') as f_n:
        lines = f_n.readlines()
        index = 0
        for el in lines:
            k = el.split(",")
            if k[0] == first_name:
                rerite(index)
            index = index + 1

def main():
    while True:
        command = input('Введите команду: w - ввод нового абонента, r - вывод на экран списка абонентов, d - удаление абонента, q - выход : \n')
        if command == 'q':
            break
        elif command == 'r':
            if not exists('phone.csv'):
                print('Файл не создан')
                break
            print(*read_file('phone.csv'))
        elif command == 'd':
            delete_entry()
        elif command == 'w':
            if not exists('phone.csv'):
                create_file()
                record_info()
            else:
                record_info()

main()