 x= 1
 x = "123"
 x = '123'
 x = 1.2
 y = 1
 a = [1,2,3]
d={'a': 1, 'b': 'qwe'};

if x > 1 :
    print('maior que um')
    if x > 2:
        print('maior que 2')
else:
    print('menor ou igual a 1')

print('voltou')

print()

'123'
print('123'+'456')

print('x = ' + str(x))

print('x = {:6.3f}, y = {}'.format(x,y))

print(f'x = {x}')

from glob import glob

for i,j in zip(range(1,4,2), range(4)):
    print(f'i = {i}, j={j}')

i = 0
while i < 10:
    print(f'i = {i}')
    i += 1

try:
    print(x)
    raise('erro')
except NameError:
    print('variável não definida')
except:
    print('Deu algum erro')
finally:
    print('ação de liberação de recursos')

a=glob('~/*~')

import matplotlib.pyplot as plt

data = [1,2,4,5,3,2,7]

plt.plot(data)
plt.xlabel('série')
plt.show()

# f(x) ?

def f(x):
    return x*x;

x=f(2.6)
s = '''
linha 1
linha 2
'''

def g(x):
    print("abc e {}".format(x))

g('string')
g([1,2,3])

# programação funcional
# efeito colateral (side effect)
s

def concat_s(s1,s2):
    global s
    s = s1 + s2
    print(s)

concat_s('1', '2')

def sqr(x):
    return x * x;

def quad_side_effect(d):
    for n in range(len(d)):
        d[n] = sqr(d[n])

def quad_no_se(d):
    e = []
    for n in range(len(d)):
        e.append(sqr(d[n]))
    return e

d2 = [1,2,3,4,5,6]

d3 = quad_no_se(d2)

d4 = list(map(sqr, d2))

h = lambda x: x*x

d5 = list(map(lambda x : x*x*x, d2))
