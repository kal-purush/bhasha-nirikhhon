# name = input('Hey guest, what do people call you? ')

# with open('samplefile.txt', 'a') as file:
#     file.write(name +'\n')


# why_programming = input('Yo dude, what motivates you to program? ')

# while why_programming != 'q':
#     with open('reasonsforprogramming.txt', 'a') as file_object:
#         file_object.write(why_programming + '\n')
#         why_programming = input('Yo dude, why do YOU like code? Press q to quit. ')

# with open('reasonsforprogramming.txt', 'r') as file_object:
#     everything_in_file = file_object.read()
#     print(f'Your answers were + {everything_in_file}')


favorite_dishes = input('My dude, list for me your favorite foods: ')

while favorite_dishes != 'q':
    with open('fave-dishes.txt', 'a') as file_object:
        file_object.write(favorite_dishes + '\n')
        favorite_dishes = input('Tell me another dish you like, or press q to quit. ')

with open('fave-dishes.txt', 'r') as file_object:
    content = file_object.read()
    print(content)