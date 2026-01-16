from random import randint

alphabet = ["A","B","C","1","2","3"]

part_one = ""
part_two = ""
Final_key = ""
i = 0

while i <=3:
    part_one = part_one + alphabet[randint(0,5)]
    part_two = part_two + alphabet[randint(0,5)]
    i = i + 1
Final_key = part_one + "-"+ part_two
print(Final_key)