# F. front_back
# Consider dividing a string into two halves.
# If the length is even, the front and back halves are the same length.
# If the length is odd, we'll say that the extra char goes in the front half.
# e.g. 'abcde', the front half is 'abc', the back half 'de'.
# Given 2 strings, a and b, return a string of the form
# a-front + b-front + a-back + b-back



# def front_back(a, b):
  # +++your code here+++
  # LAB(begin solution)
  # Figure out the middle position of each string.
#   a_middle = len(a) / 2
#   b_middle = len(b) / 2
#   if len(a) % 2 == 1:  # add 1 if length is odd
#     a_middle = a_middle + 1
#   if len(b) % 2 == 1:
#     b_middle = b_middle + 1 
#   return a[:a_middle] + b[:b_middle] + a[a_middle:] + b[b_middle:]
#   # LAB(replace solution)
  # return
  # LAB(end solution)











a='abcde'
b='qwerty'

def front_back(a,b):
  m1=int(len(a)/2)
  m2=int(len(b)/2)
  if len(a)%2==1:
    m1=m1+1
  if len(b)%2==1:
      m2=m2+1
  return a[:m1]+b[:m2]+a[m1:]+b[m2:]
print(front_back(a,b))    