"""
Q2: Jacket Weather?
Alfonso will only wear a jacket outside if it is below 60 degrees or it is raining.

Write a function that takes in the current temperature and a boolean value telling if it is raining. 
This function should return True if Alfonso will wear a jacket and False otherwise.

Try solving this problem using an if statement.
"""

def wears_jacket_with_if(temp, raining):
    """
    >>> wears_jacket_with_if(90, False)
    False
    >>> wears_jacket_with_if(40, False)
    True
    >>> wears_jacket_with_if(100, True)
    True
    """
    #"*** YOUR CODE HERE ***"
    if temp<60 or raining==True:
        return True
    else:
        return False

if __name__ == "__main__":
    import doctest
    doctest.testmod()