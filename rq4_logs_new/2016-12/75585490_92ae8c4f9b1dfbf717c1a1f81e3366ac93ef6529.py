// Calculate average of list except first and last value

def drop_first_last(grades):
    first, *middle, last = grades
    return sum(middle)/len(middle)
print(drop_first_last([1,2,2,2,3]))