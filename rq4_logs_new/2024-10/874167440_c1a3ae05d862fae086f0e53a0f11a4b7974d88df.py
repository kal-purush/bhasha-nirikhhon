person = "salman"
print(f"my name is {person}")

d = {'a':123, 'b':456}
print(f"my number is {d['a']}")

mylist = [0,1,2]
print(f"my number is {mylist[0]}")

library = [('Author', 'Topic', 'Pages'), ('Twain', 'Rafting', 601), ('Feynman', 'Physics', 95), ('Hamilton', 'Mythology', 144)]
for author, topic, pages in library:
  print(f"{author} {topic} {pages}")

library = [('Author', 'Topic', 'Pages'), ('Twain', 'Rafting', 601), ('Feynman', 'Physics', 95), ('Hamilton', 'Mythology', 144)]
for author, topic, pages in library:
  print(f"{author:{10}} {topic:{30}} {pages:{10}}") #here 10,30, and 10 are minimum space

"""
Author     Topic                          Pages     
Twain      Rafting                               601
Feynman    Physics                                95
Hamilton   Mythology                             144
"""

library = [('Author', 'Topic', 'Pages'), ('Twain', 'Rafting', 601), ('Feynman', 'Physics', 95), ('Hamilton', 'Mythology', 144)]
for author, topic, pages in library:
  print(f"{author:{10}} {topic:{30}} {pages:>{10}}")

"""
Author     Topic                               Pages
Twain      Rafting                               601
Feynman    Physics                                95
Hamilton   Mythology                             144
"""



