from requests import get


# Функция для вывода курсов валют, полученных с API
def currency_rates(value):
    """
    Написать функцию currency_rates(), принимающую в качестве аргумента код валюты
    (например, USD, EUR, GBP, ...) и возвращающую курс этой
    валюты по отношению к рублю. Использовать библиотеку requests.
    В качестве API можно использовать http://www.cbr.ru/scripts/XML_daily.asp.
    """
    # Создаём два пустых списка, для данныч с API
    CharCode_list = []
    value_list = []
    # Получение данных API
    response = get('http://www.cbr.ru/scripts/XML_daily.asp')
    # Меняем декодируем бинарный код полученных данных
    content = response.content.decode(encoding=response.encoding)
    # Проверка кода валюты - добавляем в список, если есть совпадение
    for el in content.split('<CharCode>')[1:]:
        CharCode_list.append(el.split('</CharCode>')[0])
    # Возвращем курс валюты и переводим в число меняя разделитель дробной части
    for item in content.split('<Value>')[1:]:
        value_list.append(item.split('</Value>')[0].replace(',', '.'))
    # Записываем значения в словарь
    pairs_dict = dict(zip(CharCode_list, value_list))
    # Возвращаем словарь
    return pairs_dict.get(value.upper())


print(currency_rates('USD'))
print(currency_rates('EUR'))
print(currency_rates('GBP'))