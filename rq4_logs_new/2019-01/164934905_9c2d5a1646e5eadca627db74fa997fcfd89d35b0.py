"""
This module demonstrates how to INPUT from the CONSOLE:
  -- ints (integers)
  -- floats (floating point numbers)
  -- strings.

Authors: David Mutchler, Vibha Alangar, Matt Boutell, Dave Fisher,
         Mark Hays, Amanda Stouder, Aaron Wilkin, their colleagues,
         and PUT_YOUR_NAME_HERE.
"""  # TODO: 1. PUT YOUR NAME IN THE ABOVE LINE.

###############################################################################
# TODO: 2.  Read and run this program.  Then do the following problems,
#   putting your answers RIGHT HERE IN THIS DOCUMENT.
#
#   1. Write a line of code that would input an INTEGER from the
#      console, storing the integer in a variable called 'x'.
#      Write your line here:
#
#   2. Write a line of code that would input an FLOAT from the console,
#      storing the float in a variable called 'x'.
#      Write your line here:
#
#   3. Write a line of code that would input an STRING from the console,
#      storing the string in a variable called 'x'.
#      Write your line here:
#
#   4. What happens if you (the user) enter something OTHER than a
#      single integer (e.g., you enter
#        five
#      or
#        4.5
#      or
#        1 1 1
#      or
#        nothing at all (just press the Enter key)
#      -- try them!) when running the   input_an_integer   example?
#      Put your answer here:
#
#   After you have PUT YOUR ANSWERS IN THIS COMMENT as described above,
#     a. Find someone who has had HER answer checked.
#        Ask her to check YOUR answers to the above.
#     b. Change the above TO DO to DONE.
#
#   As always, ask questions as needed!
###############################################################################


def main():
    """ Calls the other functions in this module to demo CONSOLE IO. """
    input_a_string()
    input_an_integer()
    input_a_float()


###############################################################################
# Example: how to INPUT a STRING from the Console.
###############################################################################
def input_a_string():
    print()
    print('--------------------------------------------------')
    print('Demonstrating  CONSOLE INPUT   of a STRING:')
    print('--------------------------------------------------')

    #----------- Using the   INPUT  function ----------------------------------
    name = input('Enter your name: ')
    #--------------------------------------------------------------------------

    print('Hi, ' + name + '!  ', name, '!.  ', name)
    print('  Sorry, I have the hiccups...')


###############################################################################
# Example: how to INPUT an INTEGER from the Console.
###############################################################################
def input_an_integer():
    print()
    print('--------------------------------------------------')
    print('Demonstrating  CONSOLE INPUT   of an INTEGER:')
    print('--------------------------------------------------')

    #----------- Using the   INPUT  and   INT   functions ---------------------
    age = int(input('How old are you? '))
    #--------------------------------------------------------------------------
    print('That is ' + str(age * 12) + ' months!')
    if age >= 18:
        print('You are old enough to vote, nice!')
    else:
        print('You will be able to vote in ' + str(18 - age) + ' years.')


###############################################################################
# Example: how to INPUT a FLOAT (floating point number) from the Console
###############################################################################
def input_a_float():
    print()
    print('--------------------------------------------------')
    print('Demonstrating  CONSOLE INPUT   of a FLOATING POINT number:')
    print('--------------------------------------------------')

    #----------- Using the   INPUT  and   FLOAT   functions -------------------
    money = float(input('How much money do you have? '))
    #--------------------------------------------------------------------------

    potatoes_today = round((money / 6.46) * 10)
    potatoes_1900 = round((money / 0.140) * 10)

    print('According to Infoplease')
    print('at http://www.infoplease.com/ipa/A0873707.html')
    f_string1 = '  -- That would buy you {} pounds of potatoes in 2015.'
    f_string2 = '  -- That would buy you {} pounds of potatoes in 1900!'
    print(f_string1.format(potatoes_today))
    print(f_string2.format(potatoes_1900))


# -----------------------------------------------------------------------------
# Calls  main  to start the ball rolling.
# -----------------------------------------------------------------------------
main()