#Find the length of the longest switching sub-array. An array is called switching if all numbers in even positions are equal and all numbers in odd positions are equal. 

#Match: 

#Plan:
#loop through every element and compare with 2 element before it
# if they are the same, count += 1
# else, count = 2
# max_count = max(count, max_count)

def longest_switching_array(arr):
  n = len(arr)
  if n <= 2:
    return n
  count = max_count = 2
  
  for i in range(2, n):
    if arr[i] == arr[i-2]:
      count += 1
    else:
      count = 2
    max_count = max(count, max_count)
  return max_count

if __name__ == '__main__':
  #case 1
  arr = [0,1,2,3]
  expect = 2
  print('case1', expect, longest_switching_array(arr))
  

  #case 2
  arr = [0,1,0,1]
  expect = 4
  print('case2', expect, longest_switching_array(arr))
  
  #case 3
  arr = [4,0,1,0,1,9]
  expect = 4
  print('case3', expect, longest_switching_array(arr))
  
  #case 4
  arr = [4]
  expect = 1
  print('case4', expect, longest_switching_array(arr))
  
  #case 5
  arr = []
  expect = 0
  print('case5', expect, longest_switching_array(arr))
  
  #case 6
  arr = [-1,-1]
  expect = 2
  print('case6', expect, longest_switching_array(arr))