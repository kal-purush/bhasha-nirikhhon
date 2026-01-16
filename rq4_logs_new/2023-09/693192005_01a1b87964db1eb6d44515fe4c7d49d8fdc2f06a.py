print("Введите кол-во строк:")
a = int(input())
print("Введите кол-во символов:")
b = int(input())
mas = []
for i in range(a):
    # print('Введите ', b, ' цифр')
    test = list(map(int, input().split()))
    if len(test) == b:
        mas.append(test)
    # print('Fihish iteration')

print(mas)
num_max = mas[0][0]
best_i, best_j = 0, 0
for i in range(a):
    for j in range(b):
        if mas[i][j] > num_max:
            num_max = mas[i][j]
            best_i, best_j = i, j
print(best_i, best_j)

# m = []
# result = {}
# for i in mas:
#     max_value = 0
#     # print(max(i))
#     for elem in i:
#         if elem > max_value:
#             max_value = elem
#     m.append({"max": max_value, "child": mas.index(i)})
# print(m)
# max_item = 0
# for item in m:
#     if item["max"] > max_item:
#         max_item = item["max"]
#         result = item
#
# print(result["child"])



