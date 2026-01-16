# 1. Организуйте архитектуру приложения “База данных” (псевдо). В роли базы данных у вас
# будет класс  Database, который будет хранить данные в виде переменной списка.
# 2. Класс Database должен иметь методы read_data(criteria),  write_data(element).
# 3. Для элемента данных напишите класс Data. В данном случае мы будем хранить данные о
# пользователях. Data будет иметь атрибуты: country, name, age, gender, height, weight.
# 4. В классе Database метод read_data будет принимать на вход аргумент criteria, который
# является  словарем  вида  {“age”:  25},  после  чего  метод  вернет  отдельный  список  всех
# элементов у которых данное условие истино.

class Data:
    _country = ''
    _name = ''
    _age = 0
    _gender = ''
    _height = 0
    _weight = 0

    def __init__(self, country, name, age, gender, height, weight):
        self._country = country
        self._name = name
        self._age = age
        self._gender = gender
        self._height = height
        self._weight = weight


class Database:
    _list_database = []

    def __init__(self):
        pass

    def read_data(self, criteria):
        true_criteria = []
        for data_element in self._list_database:
            if data_element.__dict__.get('_age') == criteria.get('age'):
                true_criteria.append(data_element)
        return true_criteria

    def write_data(self, element):
        self._list_database.append(element)


if __name__ == '__main__':
    data_1 = Data('Ukraine', 'Oleksii', 55, 'Men', 180, 87)
    data_2 = Data('Russia', 'Aleksey', 35, 'Men', 175, 80)
    data_3 = Data('England', 'Anna', 45, 'Women', 165, 63)
    data_4 = Data('Australia', 'Alex', 33, 'Men', 183, 90)
    data_5 = Data('China', 'Anchun', 15, 'Men', 169, 102)
    database_1 = Database()
    database_1.write_data(data_3)
    database_1.write_data(data_2)
    database_1.write_data(data_4)
    database_1.write_data(data_1)
    database_1.write_data(data_5)
    criteria_age = {'age': 25}

    result = database_1.read_data(criteria_age)

    if result.__len__() > 0:
        for element_data in result:
            print(element_data.__dict__.get('_name'), element_data.__dict__.get('_age'))
    else:
        print("Don't have person from true this criteria!")