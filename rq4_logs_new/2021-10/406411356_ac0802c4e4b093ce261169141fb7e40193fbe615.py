# Напишите функцию, которая рекурсивно ищет в сложном объекте значение. 
# Сложный объект — это будет список списков списков и т.д. Например, [1, 2, [3, 4, [5, [6, []]]]]. 
# Функция должна рекурсивно заходить внутрь вложенных массивов, а если это другой тип данных 
# игнорировать его.


def search_varieble(object, variable):
    if type(object) is list:
        for element in object:
            if type(element) is list:
                search_varieble(element, variable)
            elif element == variable:
                print("Searching element is find :)")
                break


def find_variable(array, search, index=0):
    if index < len(array) and type(array[index]) is list:
        find_variable(array[index], search, index=0)
    elif index < len(array):
        if array[index] == search:
            print("Searching element is find :)")
        index += 1
        find_variable(array, search, index)        


list_value = [1, 2, [3, 4, [5, [6, []]]], 8]
search_varieble(list_value, 8)
    
find_variable(list_value, 8)