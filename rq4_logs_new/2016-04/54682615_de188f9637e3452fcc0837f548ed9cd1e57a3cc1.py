hours = raw_input('Enter Hours: ')
hours = float(hours)
rate = raw_input('Enter Pay Rate: ')
rate = float(rate)
if hours > 40.0:
#The first 40 hours should be the normal rate, and after 40 hours
#The rate should be 1.5
#41 , 40 x rate + (41-40) x 1.5 x rate
	total = (40 * rate) + ((rate*1.5) * (hours-40))
elif hours <= 40.0:
	total = hours * rate

print "Your Gross Pay is: ", total