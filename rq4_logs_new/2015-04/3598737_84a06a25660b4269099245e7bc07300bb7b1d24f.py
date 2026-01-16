from __future__ import print_function  # Force use print()
import os
import sys
sys.path.append('..')
sys.path.append('.')
import rename

extensions = ('.gif', '.png', '.jpg')

def cleanup_fake():
    os.chdir("./")
    for i in os.listdir("."):
        # Check if it's a .gif, .png or .jpg and it's size is 0
        if i.endswith(extensions) == True and os.stat(i).st_size == 0:
            os.remove(i)

cwd = os.getcwd()
if cwd.endswith("/fake_images"):
    print("\nYou are in {0}".format(cwd))
    print("Run this instead from parent /Image-Renamer directory.\n")
    sys.exit(0)
if "rename.py" in os.listdir("."):
    print("\nCurrent folder:", cwd)
    answer = rename.yes_or_no("Remove fake images in current folder?: ")
    print("")
    cleanup_fake()
else:
    sys.exit(
        "\nRun this instead from the directory which contains rename.py\n")