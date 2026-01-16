import copy
import itertools

def qsort(L, l, r): # L[l..r) = L[l], L[l+1], ..., L[r-1] をソート
    if l == r: return # 空部分リストのソートなので、何もしない

    pivot = L[l] # 左端をpivotにする
    i = l+1
    j = r-1 # 初期位置をセット

    # Partitioning part
    while True:
        while i < j and L[i] < pivot:
            i += 1
        while l < j and L[j] >= pivot:
            j -= 1
        if j <= i: break
        else:
            L[i], L[j] = L[j], L[i] # 値の入れ替え

    L[l] = L[j]
    L[j] = pivot

    qsort(L, l, j)
    qsort(L, j+1, r)

# 値がM未満の長さsizeのリストを片っ端から生成して
# 正しくソートできているか検証する
    
M = 7
for size in range(0, 8):
    for tup in itertools.product(range(M), repeat=size):
        L = list(tup) # 値がM未満の長さsizeのリストを片っ端から生成
        # print(L)
        orig = copy.deepcopy(L)
        qsort(L, 0, len(L))
        if sorted(orig) != L: # 整列されているかをテスト
            print("Bug:", orig)
            return