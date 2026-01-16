
# a=10
# b=0
# c=a/b
# print(c)


# try:
#     a=10
#     b=0
#     c=a/b
#     print(c)
   
# except Exception as e:
#     print(e)
#     print("Nandhakumar")


# a=10
# b=0
# c=a/b
# # print(c)    
# try:
#     a=10
#     b=0
#     c=a/b
#     print(c)
# except ZeroDivisionError as e:
#     print("ZerodivisionError",e)
#Name Error:
# try:
#     name=123
#     print(name)
# except NameError as e:
#     print("Name error",e) 
#value Error
# try:
#     num=int(input("Enter the number"))
#     print(num)
      
# except ValueError as e:
#     print("VAlue error:",e)
#     print("krish")
#Index error
# try:
#     li=[10,45,20,30,48]
#     print(li[6])
# except IndexError as e:
#     print("Index Error",e)
#KEY ERROR
try:
    dd={"Name":"sam","name1":"pk"}
    print(dd['age'])
except KeyError as e:
    print("KEy error:",e)
    print("Annamalai")
#Attribute Error:
# try:
#     class a:

#         pass
#     A=a()
#     a.hello()
# except AttributeError as e:
#     print("Attribute Error:",e)