# calculator made with basic methods using Python

print "Welcome to PyCalc"
    # greeting
operator = input("Please enter 1 for Addition, 2 for Subtraction, 3 for Multiplication, 4 for Division"
                 "\nand 5 for exponential multiplications\n")
num1 = input("First number: ")
num2 = input("Second number: ")
something = 1
somethingElse = 2

if operator == 1:
    # remember to use == for comparing two different variables
    print num1+num2
if operator == 2:
    print num1-num2
if operator == 3:
    print num1*num2
if operator == 4:
    print num1/num2
if operator == 5:
    print num1**num2
    # the ** operator is the power function which is essentially n^n.

if something == somethingElse:
    print "Hello!"