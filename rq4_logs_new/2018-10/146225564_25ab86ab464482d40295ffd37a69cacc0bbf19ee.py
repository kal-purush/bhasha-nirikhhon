"""
Program to sort files with various name and extensions into subdirectories for each extension\
Version 1
"""

import os
import shutil

print("Starting directory is: {}".format(os.getcwd()))
os.chdir('FilesToSort')
print(os.getcwd())
extension_to_folder = {}
for name in os.listdir('.'):
    extension = name[name.find('.') + 1:]
    if extension not in extension_to_folder:
        # extensions.append(extension) add to dictionary
        os.mkdir(extension)
    shutil.move(name, extension + '/' + name)
    print(extension)