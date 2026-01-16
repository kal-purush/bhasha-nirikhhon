'''
Created on Mar 1, 2015

@author: mart00n
'''
# Recursive function that takes only integer input (anything else is an error)
# the integer defines how many lines of the GTK recursive acronym print
# The function should take two inputs, number of layers and then 
# 1 >>> Gtk
# 2 >>> Gimp toolkit
# 3 >>> GNU Image manipulation toolkit
# 4 >>> GNU is not Unix Image manipulation toolkit
# 5 >>> GNU is not Unix is not Unix Image manipulation toolkit
# ... ... ...
def gtkfxn(n):
    print(n)

gtkfxn('Hello')