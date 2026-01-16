#This is varibale that defines what an object is. 
#Variables are defined with the assignment operator "=" 
#Tuples are always defined as thing comma thing comma thing
programming_languages = "Python", "Java", "C++", "C#"
#This defines what type of object "programming_languages is
#in this case, it is a tuple 
#which is immutable and can not be changed.
print(type(programming_languages))
#language becomes a temp variable on the next line
#Source: https://docs.python.org/3/tutorial/datastructures.html#tuples-and-sequences
for language in programming_languages:
    print(language)