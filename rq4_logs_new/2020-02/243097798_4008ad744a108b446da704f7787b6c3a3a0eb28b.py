my_list = [100, 200.34, 5+6j, "обычная строка", False, None, [1, 2, 3], {"a", "b", "c"}, frozenset({'a', 'k', 'b', 'd', 'r'}), {'key_1':'val_1','key_2':'val_2'}]
print(my_list)
print(type(my_list))
i=1

for el in my_list:
    print(f"{i}-й элемент списка:")
    print(el)
    print("имеет тип: ", type(el))
    i+=1
    print("===================================")