'''
Given an integer numRows, return the first numRows of Pascal's triangle.

In Pascal's triangle, each number is the sum of the two numbers directly above it as shown:

Example 1:

Input: numRows = 5
Output: [[1],[1,1],[1,2,1],[1,3,3,1],[1,4,6,4,1]]
Example 2:

Input: numRows = 1
Output: [[1]]
'''


def pascals_triangle(n):
    res = [[1]] #initializing the default case
    for i in range(n-1): #Since the array is already initialized we need one less element
        temp = [0] + res[-1] + [0] #Adding zeros to the ends to get a cleaner reference
        l = []
        for j in range(len(res[-1])+1):
            l.append(temp[j]+temp[j+1]) #Getting the value for the cell using the j and j+1th element.
        res.append(l)
    return res