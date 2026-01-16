#hashmap = {}
#traverse the input list
    #sort element - "eat" -> "aet"
    #if sorted element is in hashmap
        #add the element to hashmap {"aet":[]} according to key
    #else:
        #add the sorted element to hashmap as a key and empty array as value
#loop through hashmap and append value into an array
#return appended array

arr = ["eat","tea","tan","ate","nat","bat"]
def group_anagrams(arr):
  length = len(arr)
  if length == 0:
    return arr
  dic = {}
  for elem in arr:
    target = ''.join(sorted(elem))
    if target in dic:
      dic[target].append(elem)
    else:
      dic[target] = [elem]
  return dic.values()

#Output: [["bat"],["nat","tan"],["ate","eat","tea"]]

#Evaluate
#Runtime: O(n)
#Space: O(n)