# This Python script tests all of the different definitions

# and
boolean = True and False
print boolean 
print "\n"

#as? Not sure how the "with-as" works

# assert
test = 5
assert (test == 5), "This is not correct"

print "If you are reading this then the assertion is correct."
print "\n"

# break
loop = 0
while True:
	if loop < 5:
		loop += 1
		print "loop = %d" % loop
	else:
		print "Here is the break."
		print "\n"
		break


# class
class Person(object):
	object = ' "test_variable" '
	print "Here is the person class with a passed in variable that = %s" % object
	print "\n"
	
# continue
var = 9
while var < 15:
	var += 1
	if var == 13:
		print "13 is unlucky, so we are skipping it."
		continue
	print "The current number is %d" % var
print "We are all done."
print "\n"
