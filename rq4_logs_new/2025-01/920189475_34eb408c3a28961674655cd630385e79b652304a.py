

def prompt_and_print():
    x= int( input ("Enter a number:") ) 
    y= int( input ("Enter a number:") )

    addresult= x+y
    subresult= x-y
    multiresult= float(x*y)
    divisionresult= float(x/y)
    print("Addition: " , addresult , "Subtraction:" , subresult , "multiplication:" , multiresult , "division:" , divisionresult)

prompt_and_print()