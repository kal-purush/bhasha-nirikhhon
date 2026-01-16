# на Отлично в одного человека надо сделать консольное приложение Телефонный 
# справочник с внешним хранилищем информации, и чтоб был реализован основной 
# функционал - +просмотр, +сохранение, ?импорт, +поиск, +удаление, +изменение данных.

# Телефонный справочник

import json
# Блок работы с файлами
# функция загрузки из файла информации
def read_file():
    with open('phonebook.json', 'r') as f:
        data = f.read()
        book = eval(data)
        return(book)

# функция переписывает содержимое файла
def write_file(dict):
    with open('phonebook.json', 'w') as f:
        f.write(json.dumps(dict))

# Блок работы со словарем
# Ищем существующий контакт
def find_contact():
    name = input('Введите имя контакта: ')
    data = read_file()
    if len(data) == 0:
        print("В телефонной книге нет контактов")
    else:
        if name in data:
            value = data[name]
            print(f'У абонента "{name}" номер телефона: {value}')
        else:
            print(f'Контакт "{name}" в телефонном справочнике отсутствует')

#Сохраняем новый контакт
def save_contact():
    data = read_file()
    name = input("Введите имя контакта: ")
    number = input("Введите номер телефона без пробелов: ")
    data[name] = number
    print('Контакт добавлен!')
    write_file(data)

# Изменить контакт
def change_contact():
    data = read_file()
    name = input("Введите имя контакта, которое хотите изменить: ")
    if name in data:
        new_name = input("Введите новое имя контакта: ")
        new_number = input("Введите новый номер контакта: ")
        # тут будет ссылка на функцию удаления
        del data[name]
        data[new_name] = new_number
        print('Контакт изменен!')
        write_file(data)
    else:
        print('Такого контакта нет, но вы можете его создать используя функцию "сохранить".')

# Удалить контакт    
def delete_contact():
    data = read_file()
    name = input("Введите имя контакта, которое хотите удалить: ")
    if name in data:
        del data[name]
        print('Контакт удалён!')
        write_file(data)



save_contact()
print(read_file())
change_contact()
print(read_file())
delete_contact()
print(read_file())

