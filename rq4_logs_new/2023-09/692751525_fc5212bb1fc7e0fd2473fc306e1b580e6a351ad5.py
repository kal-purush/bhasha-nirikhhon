number_one = float(input('First number: '))
number_two = float(input('Two number: '))
formula = input('Operation: ')
if formula == '1':
    Result = number_one + number_two
elif formula == '2':
    Result = number_one - number_two
elif formula == '3':
    Result = number_one * number_two
elif formula == '4':
    Result = number_one / number_two
print(f'Result: {Result}')
# first_task
def palindrome(pal):
    pal = pal.replace(" ", "").lower()
    return pal == pal[::-1]
checked = input("Write string:")
if palindrome(checked):
    print("Palindrome")
else:
    print("No Palindrome")
# second_task
my_list = ['Hillel', 'AQA', 'TEST']
string_list = ','.join(my_list)
return_list = string_list.split(',')
print("List as String:", string_list)
print("Return List:", return_list)
# third_task
original_list = ['Tst', 'aBc', 'TEST', 'Hello', 'neW']
original_list.sort(key = str.lower)
print("Original List:", original_list)
# four_task
original_list = [1, 2, 3, 4, 5, 6, [1, 2, 3, ['Win']]]
new_list = original_list[6][3]
print("New List:", new_list)
# five_task
my_list = input(list)
sorted_my_list = my_list == sorted(my_list)
print("My List:", my_list)
if sorted_my_list:
    print("My list is sorted.")
else:
    print("My list is not sorted.")
# six_task