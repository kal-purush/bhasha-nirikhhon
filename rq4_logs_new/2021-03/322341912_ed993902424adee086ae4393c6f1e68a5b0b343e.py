import os
import tika
from tika import detector, parser
tika.initVM()

mydir = (r'C:\Users\thexa\Documents\Development\Python\goldenlog\testfiles')

uniqueKeys = list()

for subdir, dirs, files in os.walk(mydir):
    for filename in files:
        filepath = subdir + os.sep + filename

        parsedfile = parser.from_file(filepath)
        filetype = detector.from_file(filepath)
        metadata = parsedfile["metadata"]

        allKeys = list()
        for k in metadata.keys():
            k = k.lower()
            allKeys.append(k)

        for k in allKeys:
            if k not in uniqueKeys:
                uniqueKeys.append(k)

        uniqueKeys.sort()

print(uniqueKeys)


# convert output to .docx file to avoid utf-8 character errors. save list to ukeys.py