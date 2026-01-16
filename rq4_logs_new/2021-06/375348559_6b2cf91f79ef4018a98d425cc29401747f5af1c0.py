# 6.一个数如果恰好等于它的因子之和，这个数就称为"完数"。
#  例如6=1＋2＋3.编程找出1000以内的所有完数。（提示：分解因数）

# num = int(input("input num:"))
for i in range(1, 1001):
    ls = []
    for j in range(1, i):
        if i % j == 0:
            ls.append(j)
    sum_list = sum(ls)
    if sum_list == i:
        print(i)