"""
Задача 44: В ячейке ниже представлен код генерирующий DataFrame, которая состоит всего из 1 столбца. Ваша задача
перевести его в one hot вид. Сможете ли вы это сделать без get_dummies?

"""

import random
import pandas as pd

lst = ['robot'] * 10
lst += ['human'] * 10
# print(lst)
random.shuffle(lst)
data = pd.DataFrame({'whoAmI':lst})
data.head()
# print(data)
data1 = pd.DataFrame({'whoAmI_human':lst,'whoAmI_robot':lst})
# print(data1)
for i in range(len(lst)):
    if data['whoAmI'].values[i] == 'robot':
        data1['whoAmI_human'].values[i] = 0
        data1['whoAmI_robot'].values[i] = 1

    if data['whoAmI'].values[i] == 'human':
        data1['whoAmI_human'].values[i] = 1
        data1['whoAmI_robot'].values[i] = 0
# print(data)
# print(pd.get_dummies(data))
print(data1)

print(pd.Series(list('abca')))