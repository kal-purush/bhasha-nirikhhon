# From 3 Python Automation Projects - For Beginners
# By Tech With Tim
# Project #1 - Multi-clipboard

# an application to store multiple things in the copy/paste clipboard

# Import modules
import sys
import clipboard
import json

# reference the clipboard module

# data = clipboard.paste() # Note 1
# clipboard.copy("abc") # note 2

# passing command line arguments to the python file
# python 1_multiclip.py test # note 3

# to access the argument in python
print(sys.argv[1:])  # note 4

# an if statement to check to ensure the user has passed in only 1 command.
if len(sys.argv) == 2:





''' Notes:
1) Pastes the Data from your clipboard to the Data variable.
2) To save Data from the users clipboard
3) to pass the arguments to python - 1, call up python [python or python3],
	2, identify they program [1_muliticlip.py], 3 pass the argument [test].
4) sys.argv will give you all the command line arguments that is passed in along side
	take a slice so it doesnt return the file name[1:] 
5) 
6) 	

'''