import os

path = ''#path of the file dir
fileExtensionToRemove = ''#extension of the files to be renamed
renamePrefix = ''#if any prefix is needed
renamePostfix = ''#if any postfix is needed

for directories, dirNames, files in os.walk(path):
    for names in files:
        name, extension = os.path.splitext(names)
        if(extension == fileExtensionToRemove):
            os.rename(path+'\\'+names, path+'\\'+renamePrefix+names)