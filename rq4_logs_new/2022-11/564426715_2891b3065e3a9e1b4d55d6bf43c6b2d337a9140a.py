
def qsort(lst: list) -> list[int]:
    if len(lst) < 2:
        return lst 
    pivot = lst[0]
    less = [i for i in lst[1:] if i < pivot]
    greater = [i for i in lst[1:] if i > pivot]
    return qsort(less) + [pivot] + qsort(greater)

print(qsort([4, 3, 8, 9, 10, 11]))