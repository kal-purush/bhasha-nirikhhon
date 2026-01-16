per_cent = {'ТКБ': 5.6, 'СКБ': 5.9, 'ВТБ': 4.28, 'СБЕР': 4.0}
a = input('Введите сумму: ')
b = per_cent["ТКБ"] * float(a)
c = per_cent["СКБ"] * float(a)
d = per_cent["ВТБ"] * float(a)
e = per_cent["СБЕР"] * float(a)

deposit = [b, c, d, e]
print ("Сумма депозита составит: ",deposit)
max_number = max(deposit)
print("Максимальная сумма, которую вы можете заработать:", max_number)