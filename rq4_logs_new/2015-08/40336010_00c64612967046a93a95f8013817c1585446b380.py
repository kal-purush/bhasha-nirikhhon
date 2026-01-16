'''
Checks a string of text for a curse word using the wdyl website.
Add the ability to check against a dictionary for misspelled words

'''
import urllib
import os

def begin():
    print("Python command line program for checking misspelled words aswell as profanity ")
    try:
        var1 = input('Would you like to check a file using a DICTIONARY or PROFANITY checker for errors? \n>>>')
        if var1.lower() == 'dictionary':
            dictionary()
        
        elif var1.lower() == 'profanity':
            usr_inp = input("File to be checked for profanity?(e.g C:\root\python\Scripts\file.txt) \n")
            read_text(usr_inp)
            
        else:
            exit(0)
    except ValueError:
        print("Error", exit(0))
    
def read_text(usr_inp2):
    #user_input = raw_input('File to check? \n')
    #quotes = open(r'C:\Python27\bad_word.txt') # reads a file with text

    file_name = open(usr_inp2)
    contents_of_file = file_name.read()
    #print(contents_of_file)
    check_profanity(contents_of_file) #passes contents_of_file through check_profanity function
    file_name.close()

def check_profanity(text_to_check):
    connection = urllib.urlopen('http://www.wdyl.com/profanity?q=' + str(text_to_check)) # opens/establishes connection
    output = connection.read()
    print(output)
    connection.close()
    try: # checks if profanity is prescent in document
        if 'true' in output:
            print('Profanity!')
        elif 'false' in output:
            print('No profanity located')
    except ValueError:
        print("Error checking document")

def dictionary():
    print("Dictionary")


begin()
#read_text()
#check_profanity(raw_input('Text to check? \n>>> '))