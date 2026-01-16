import collections
'''
https://stackabuse.com/introduction-to-pythons-collections-module/
'''

#counter
# used to convert list to dict as count of presence of each element
list1=[1,2,2,3,3,3,4,44,5,6,1,6,5,4,4,4,'2']
list_cnt=collections.Counter(list1)
print list_cnt


## elements function will convert dict to list
dict={1:3,3:8}
dict_cnt=collections.Counter(dict)
z= list(dict_cnt.elements())
print z




Student = collections.namedtuple('Student', 'fname, lname, age')
s1 = Student('John', 'Clarke', '13')
print(s1.fname)
