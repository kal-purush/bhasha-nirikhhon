# script to check if string has unique characters

def isUniqueChar(str):
	# if string has more than 128 characters than it definitely has duplicate characters
	# since we have 128 unique characters in ascii

	if len(str) > 128:
		return False

	# create an array for holding each of 128 unicode characters
	# assign False to each initially
	# then assign True while looping through the string if a charcter is found
	# ord return unicode value of a character
	arr = [False] * 128
    
	for each_char in str:
		if arr[ord(each_char.lower())] is False:
			arr[ord(each_char.lower())] = True
		else:	
			return False

	return True


testString = "abcdA"

print "String " + testString + " has Unique characters? " + str(isUniqueChar(testString))


#similar approach using Hash table
def isUnique(str):
  char_Hash = {}
  # create a hash table for all characters and assign them to False
  for i in range(ord('a'),ord('z')+1):
    char_Hash[chr(i)] = False
  
  # if value for char is found True then it is appearing twice
  for i in range(len(str)):
    if char_Hash[str[i]] == True:
      return False
    else:
      char_Hash[str[i]] = True
  
  return True

print isUnique('abdc'.lower())
