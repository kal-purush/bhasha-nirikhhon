pizzas = ['queijo', 'portuguesa', 'mista', '4 queijos', 'frango']
friend_pizzas = pizzas[:]
pizzas.append('calabresa')
friend_pizzas.append('banana')

print('Minhas pizzas favoritas são: ')
for i in pizzas:
    print(f"- {i}")

print('\nAs pizzas favoritas dos meus amigos são: ')
for friends in friend_pizzas:
    print(f'- {friends}')

