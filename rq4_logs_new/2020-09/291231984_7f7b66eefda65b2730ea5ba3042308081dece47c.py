# Создать текстовый файл (не программно), сохранить в нем несколько строк, выполнить подсчет количества строк, количества слов в каждой строке.

try:
    # Файл д.б. создан после выполнения первой задачи
    my_f = open("file1.txt", "r", encoding="utf-8")

    cnt = 0
    lst = my_f.read().split("\n")
#    print(lst)
    for line in lst:
        lst2 = line.split(' ')
        print(f"строка: {line}. Количество слов = {len(lst2)}")
        cnt += 1
    print(f"Количество строк: {len(lst)}")
finally:
    my_f.close()