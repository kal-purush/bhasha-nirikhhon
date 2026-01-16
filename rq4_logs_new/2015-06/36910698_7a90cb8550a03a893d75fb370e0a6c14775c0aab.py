# Exercise 15 - Reading Files
# accompanied by ex15_sample.txt

from sys import argv # Open module

script, filename = argv # Define variables/arguments

txt = open(filename) # Declare 'value' of txt

print "here's your file %r:" % filename
print txt.read() # Print contents of 'filename'

print "Type the filename again:"
file_again = raw_input("> ") # Access file through prompt

txt_again = open(file_again) # Add file text to 'txt_again' variable

print txt_again.read() # Print text

txt.close() # Close access to file
txt_again.close() # Close access to file