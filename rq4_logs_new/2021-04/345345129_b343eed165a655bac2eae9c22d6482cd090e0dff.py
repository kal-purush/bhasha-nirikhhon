f = open("myfile.txt", 'w+')
f.write("I love Python\n It's cool lang")
f.close()

fil = open("myfile.txt", 'r')
str1 = fil.read()
print(str1)
print(fil.tell())
fil.seek(10)
print(fil.tell())
str2 = fil.read()

print(str2)
fil.close()