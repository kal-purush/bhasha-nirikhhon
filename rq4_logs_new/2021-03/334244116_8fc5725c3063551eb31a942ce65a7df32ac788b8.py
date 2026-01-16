import re
file = open('exp.txt', 'r')
Lines = file.readlines()


for line in Lines:
    result = re.search("^(\s*[a-zA-z0-9(){}\[\]\-;:+=_\"\'.]+\s*)+$", line)
    if result:
        print(line)