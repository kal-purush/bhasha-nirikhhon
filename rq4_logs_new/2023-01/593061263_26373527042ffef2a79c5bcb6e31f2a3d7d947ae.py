a = "hello world"
b = a.split()
print(f'{b[0]} {b[1]}')
print(f'!{b[1]} ! {b[0]}!')


a = "hello"
b = "world"
c = "{0} {1}".format(a, b)
d = "!{1} ! {0}!".format(a, b)
print(c)
print(d)



a = 'hello world'
b = a.split()
print('%s %s'% (b[0], b[1]))
print('!%s ! %s!'% (b[1], b[0]))
