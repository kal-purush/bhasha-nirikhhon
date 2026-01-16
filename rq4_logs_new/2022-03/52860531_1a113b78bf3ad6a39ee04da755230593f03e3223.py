test_list=[1, 4, 5, 5, 5, 9, 1]

print("The original list is : " + str(test_list)) 
  
# Duplicate element indices in list 
# Using set() + loop 
oc_set = set()
print (oc_set)
res = [] 
for idx, val in enumerate(test_list): 
    if val not in oc_set: 
        oc_set.add(val)
        print ('oc',oc_set)
    else: 
        res.append(idx)      
          
# printing result 
print("The list of duplicate elements is :  " + str(res)) 