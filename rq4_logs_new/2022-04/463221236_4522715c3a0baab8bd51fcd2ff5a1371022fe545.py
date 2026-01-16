# Python 3.7.10
# coding UTF-8
# cleaning the preproinsulin
# remember to use the regex

import re

aminoList = []

with open('preproinsulin-seq.txt') as newFile1:
    for line in newFile1:
        for detail in line:
            if detail != "/" and detail != " " and detail != '1' and detail != '6' and detail != "\n":
                aminoList.append(detail)
    # print(aminoList)

newString = "".join(aminoList)
newString1 = re.sub(r"ORIGIN", "", newString)


if (len(newString1)) == 110:
    print(newString1)
else:
    print("Needs more cleaning")

with open('lsinsulin-seq-clean.txt', 'w') as newFile2:
    newFile2.write(newString1[0:24])
with open('binsulin-seq-clean.txt', 'w') as newFile3:
    newFile3.write(newString1[24:54])
with open('cinsulin-seq-clean.txt', 'w') as newFile4:
    newFile4.write(newString1[54:89])
with open('ainsulin-seq-clean.txt', 'w') as newFile5:
    newFile5.write(newString1[89:110])

