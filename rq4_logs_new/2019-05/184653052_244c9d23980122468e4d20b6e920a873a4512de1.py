#!/usr/local/bin/python

# --------------------------------
# Imports
# --------------------------------

import sys
import os

# --------------------------------
# Change Android PackageName
# --------------------------------

def main():
    argv_len = len(sys.argv)
    if argv_len == 2:
        arg1 = sys.argv[1]
        splitted = arg1.split('.')
        splitted_len = len(splitted)
        if (splitted_len != 3):
            show_usage()
        else:
            change_name(arg1, splitted[0], splitted[1], splitted[2])
    else:
        show_usage()

# --------------------------------
# Change Name
# --------------------------------

def change_name(new_package_name, name1, name2, name3):
    directory_name = "presentation"
    main_dir = directory_name + "/src/main"
    root_dir = main_dir + "/java/"
    
    old_name1 = os.listdir(root_dir)[0]
    old_name2 = os.listdir(root_dir + "/" + old_name1)[0]
    old_name3 = os.listdir(root_dir + "/" + old_name1 + "/" + old_name2)[0]
    
    full_old_name = root_dir + "/" + old_name1 + "/" + old_name2 + "/" + old_name3
    old_package_name = old_name1 + "." + old_name2 + "." + old_name3

    rename_all_files(full_old_name, old_package_name, new_package_name)
    rename_manifast(main_dir + "/AndroidManifest.xml", old_package_name, new_package_name)
    rename_gradle(directory_name + "/build.gradle", old_package_name, new_package_name)
    rename_dir(root_dir, name1, name2, name3, old_name1, old_name2, old_name3)

def rename_dir(root_dir, name1, name2, name3, old_name1, old_name2, old_name3):
    os.rename(root_dir + old_name1 + "/" + old_name2 + "/" + old_name3,
        root_dir + old_name1 + "/" + old_name2 + "/" + name3)
    os.rename(root_dir + old_name1 + "/" + old_name2,
        root_dir + old_name1 + "/" + name2)
    os.rename(root_dir + old_name1,
        root_dir + name1)

def rename_all_files(full_old_name, old_package_name, new_package_name):
    list_of_files = os.listdir(full_old_name)
    for f in list_of_files:
        f = full_old_name + "/" + f
        if os.path.isfile(f):
            repackage_file(f, old_package_name, new_package_name)
        elif os.path.isdir(f):
            rename_all_files(f, old_package_name, new_package_name)

def repackage_file(file_path, old_package_name, new_package_name):
    with open(file_path, 'r') as f:
        file_read = f.read()
        file_read = file_read.replace("import " + old_package_name, 
                                      "import " + new_package_name)
        file_read = file_read.replace("package " + old_package_name, 
                                      "package " + new_package_name)
        with open(file_path, 'w') as f2:
            f2.write(file_read)

def rename_manifast(manifast_file, old_package_name, new_package_name):
    with open(manifast_file, 'r') as f:
        file_read = f.read()
        file_read = file_read.replace('package="' + old_package_name, 'package="' + new_package_name)
        with open(manifast_file, 'w') as f2:
            f2.write(file_read)

def rename_gradle(gradle_file, old_package_name, new_package_name):
    with open(gradle_file, 'r') as f:
        file_read = f.read()
        file_read = file_read.replace('applicationId "' + old_package_name, 'applicationId "' + new_package_name)
        with open(gradle_file, 'w') as f2:
            f2.write(file_read)

# --------------------------------
# Extra
# --------------------------------

def show_usage():
    print(
      "USAGE:\n\n" +
      "./change_package_name.py name1.name2.name3 --> Change the packageName to name1.name2.name3\n"
      )

if __name__ == "__main__":
    main()