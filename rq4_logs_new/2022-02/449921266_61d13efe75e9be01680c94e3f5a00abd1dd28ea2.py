from itertools import zip_longest

l1 = '1-2'
l2 = '3-4-5'
l3 = '1'
l4 = '2-3-4-5-6'

new_list = list(zip_longest(l1, l2, l3, l4))

print(new_list)
s = ''
for i in new_list:
    for j in i:
        if j == None:
         s = s + ' '
        else:
            s = s + j
    
    s = s + '\n'
print(s)