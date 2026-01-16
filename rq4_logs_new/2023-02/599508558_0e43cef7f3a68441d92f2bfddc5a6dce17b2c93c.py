dict = {'a': 1, 'c': {'a': 2, 'b': {
    'x': 3, 'y': 4, 'z': 5}}, 'd': [6, 7, 8]}


def get_none():
    return None


def flatten_dict(dict):
    [*values] = dict.values()
    return values


print(flatten_dict(dict))
print(flatten_dict({'a': 42, 'b': 3.14}))
print(flatten_dict({'a': [42, 350], 'b': 3.14}))
print(flatten_dict({'a': {'inner_a': 42, 'inner_b': 350}, 'b': 3.14}))