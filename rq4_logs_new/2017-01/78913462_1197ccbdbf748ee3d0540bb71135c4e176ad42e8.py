'''
Lily likes to play games with integers and their reversals. For some integer , 
we define  to be the reversal of all digits in . For example, , , and .

Logan wants to go to the movies with Lily on some day  satisfying , 
but he knows she only goes to the movies on days she considers to be beautiful. 
Lily considers a day to be beautiful if the absolute value of the difference between  
and  is evenly divisible by .

Given , , and , count and print the number of beautiful days when Logan and Lily 
can go to the movies.

Input Format

A single line of three space-separated integers describing the respective values of 
, , and .

Output Format

Print the number of beautiful days in the inclusive range between  i and j.

Sample Input

20 23 6

Sample Output

2
'''

#returns reversed string of integer value
def reversed(x):
    
    if x == 0 :
        return ''
    else :
        return str(x%10) + reversed(x/10)


i,j,k = map(int,raw_input().strip().split(' '))
nOfDays = 0
for x in range(i,j+1) :
    reversedVal = int(reversed(x))
    if abs(reversedVal - x)% k ==0 :
        nOfDays +=1 
    
print nOfDays