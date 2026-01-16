data = [x for x in range(0, 100)]

# print(any(data))
# print(all(data))

conditions = [True, True, {}, ]
print(all(conditions))

print(any(conditions))
print(bool([]))


new_data = []

# There are at least three possible variants how to check if container
# (list, dict, set, tuple, string) is empty or not
if len(new_data) > 0:
    print("Not empty")
else:
    print("Empty")


if len(new_data):
    print("Not empty")
else:
    print("Empty")

# For this one even PyCharm recommend to simplified code
if len(new_data) == True:
    print("Not empty")
else:
    print("Empty")

# This one is strongly preferred
if new_data:
    print("Not empty")
else:
    print("Empty")
