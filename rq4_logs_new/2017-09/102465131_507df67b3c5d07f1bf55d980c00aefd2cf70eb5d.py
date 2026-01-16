# Mr. Todd has bought Andrew a new weave, so now MEST is on a hiring freeze.
# We can only afford to pay for 4 fellows at a time.
# Use a static field to keep track of how many Fellows have been created
# If  a user tries to construct a fifth Fellow, raise an exception
class Mest:
    fellow_created = 0

    def __init__(self, name):
        self.name = name
        Mest.fellow_created += 1

#  # Name = Mest(name="")
#     def to_file(self):
#         return ",".join(self.__dict__.values())
#     def fellow_list(self):
#         with open("fellow.txt",'a') as f:
#             choice = input("select one: '1' '2' '3': ")
#             Mest('name')
#             if choice == 1 or 2:
#                 if len('f') < 4:
#                     f.write('f' + "\n")
#                 else:
#                     return Mest.fellow_created
#             print("Error! You can't add another name.")
#
# name =input("Enter the name of the fellow: ")
# print(len(name))

for i in range (10):
    while i <= 3:
        Mest("name")
        print(Mest.fellow_created)
        name = input("Enter the name of the fellow you want paid: ")
        break
    else:
        print("404,Error you can't add another fellow")

print(Mest.fellow_created)



