import random

print('The number you get is the one facing the front.')

roll = random.randint(1, 6)

if roll == 1:
  print('''
        .-------.
       / o o o /|
      /_______/o|
      |       | |
      |   o   |o/
      |       |/
      '-------'
''')

elif roll == 2:
  print('''
        .-------.
       /   o   /|
      /_______/o|
      | o     |o|
      |       |o/
      |     o |/
      '-------'
''')
  
elif roll == 3:
  print('''
        .-------.
       /   o   /|
      /_______/o|
      | o     | |
      |   o   |o/
      |     o |/
      '-------'
''')
  
elif roll == 4:
  print('''
        .-------.
       /   o   /|
      /_______/o|
      | o   o | |
      |       |o/
      | o   o |/
      '-------'
''')
  
elif roll == 5:
  print('''
        .-------.
       /   o   /|
      /_______/o|
      | o   o |o|
      |   o   |o/
      | o   o |/
      '-------'
''')
  
else:
  print('''
        .-------.
       / o o o /|
      /_______/o|
      | o   o | |
      | o   o |o/
      | o   o |/
      '-------'
''')
