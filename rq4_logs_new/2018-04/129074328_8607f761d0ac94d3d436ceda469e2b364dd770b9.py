def simple_keyword():
	"""Log a message"""
	print 'you have used the simplest keyword.'

def greet(name):
        """Logs a friendly greeting to person given as argument"""
        print 'Hello %s!' % name

def multiply_by_two(number):
        """Returns the given number multiplied by two

        The result is alway;s a floation point number.
        This keyword fails if the given 'number' cannot be coverted to number."""

        return float(number) * 2

def numbers_should_be_equal(first, second):
        print '*DEBUG* Got arguments %s and %s' % (first, second)
        if float(first) != float(second):
                raise AssertionError('Given numbers are unequal!')

				
def numbers_should_not_be_equal(first, second):
        print '*DEBUG* Got arguments %s and %s' % (first, second)
        if float(first) != float(second):
                print 'Given numbers are unequal!'