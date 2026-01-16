from sys import argv

script, filename = argv

file = open(filename)
str = file.read()

def judge(c): 
    if c >= 'A' and c <= 'Z' or c >= 'a' and c <= 'z':
        return True
    else:
        return False	

def chara_count(s):
    return len(s)
	
def word_count(s):
    word_number = 0
    flag = 0
    for char in s:
        if flag == 0 and judge(char) == True:
            flag = 1
            word_number = word_number + 1
        if (flag == 1 and judge(char) == False) :
            flag = 0
    return word_number
	
print word_count(str)