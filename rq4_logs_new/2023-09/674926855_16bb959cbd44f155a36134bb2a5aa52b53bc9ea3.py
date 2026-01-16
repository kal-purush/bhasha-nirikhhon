"""
Задача 40: Работать с файлом california_housing_train.csv, который находится в папке sample_data. Определить среднюю
стоимость дома, где кол-во людей от 0 до 500 (population).

"""

import pandas as pd

data = pd.read_csv('california_housing_test.csv')
res = data.loc[data["population"] < 500, "median_house_value"]

summ = 0
for median_house_value, dat in res.items():
    summ = summ + dat
summ = summ / data.count()[0]
print(f"Средняя стоимость дома, где кол-во проживающих людей от 0 до 500 чел. = {summ}")