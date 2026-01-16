user_data = (input("Введите любое что-то: "))
line_data = ("Это строка в которую {} новую строку").format(user_data)
print(line_data)
replacement_data = ("'Замена в строке'")
line_change = ("Это строка в которую {} новую строку").format(replacement_data)
print(line_change)
print (len(line_change))
if isinstance("строка", str):
    print("Да")

