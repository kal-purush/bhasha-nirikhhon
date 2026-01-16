result = []


def divider(a, b):
    if a < b:
        raise ValueError("a меньшеb")
    if b > 100:
        raise IndexError("b більше 100")
    return a / b


data = {10: 1, 2: 5, "123": 4, 18: 1, 8: 4}


for key in data:
    try:
        res = divider(key, data[key])
        result.append(res)
    except Exception as e:
        print(f"Ошибка: {e}")
print(result)