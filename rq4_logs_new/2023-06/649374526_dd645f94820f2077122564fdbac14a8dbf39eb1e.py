

from collections import Counter
counter1 = Counter({'a': 3, 'b': 2, 'c': 1})
counter2 = Counter({'b': 2, 'c': 2, 'd': 1})

union_counter = counter1 | counter2

print(union_counter)


list_of_pairs = [('a', 3), ('b', 2), ('c', 1), ['d', 0]]
c = Counter(dict(list_of_pairs))
print(c)


from collections import deque

d = deque([1, 2, 3, 4, 5])
d.rotate(2)
print(d)




from collections import OrderedDict

d = OrderedDict()
d['a'] = 1
d['b'] = 2
d['c'] = 3
print(d.popitem())


from collections import namedtuple

Person = namedtuple('Person', ['name', 'age', 'gender'])
p = Person('Alice', 25, 'female')
print(p._replace(age=30))
print(p)