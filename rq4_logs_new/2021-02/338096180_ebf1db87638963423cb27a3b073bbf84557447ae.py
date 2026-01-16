"""1. Написать функцию num_translate(), переводящую числительные от 0 до 10 c английского на русский язык. Например:
#>>> num_translate("one")
"один"
#>>> num_translate("eight")
"восемь"
Если перевод сделать невозможно, вернуть None.
Подумайте: как и где лучше хранить информацию, необходимую для перевода: какой тип данных выбрать, в теле функции или снаружи?"""
dict_of_numbers = {
    "zero": "ноль",
    "one": "один",
    "two": "два",
    "three": "три",
    "four": "четыре",
    "five": "пять",
    "six": "шесть",
    "seven": "семь",
    "eight": "восемь",
    "nine": "девять",
    "ten": "десять"
}

def num_translate(number):
    if 65 <= ord(number[0]) <= 122:
        return dict_of_numbers.get(number.lower()).capitalize() if number[0].isupper() and number.lower() in dict_of_numbers else dict_of_numbers.get(number)
    else:
        for a, b in dict_of_numbers.items():
            if number.lower() == b:
                return a.capitalize() if number[0].isupper() else a


print(num_translate("Один"))