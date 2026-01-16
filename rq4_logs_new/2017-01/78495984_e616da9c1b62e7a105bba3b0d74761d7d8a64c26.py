import sys
import random

ans = True

while ans:
	question = raw_input("Ask the magic 8 ball a question: (press enter to quit) ")
	
	answers = random.randint(1,8)
	
	if question == "":
		sys.exit()
		
	elif answers == 1:
		print "It is certain"
		
	elif answers == 2:
		print "Outlook good"
		
	elif answers == 3:
		print "You may rely on it"
		
	elif answers == 4:
		print "Ask again later"
		
	elif answers == 5:
		print "Concentrate and ask again"
		
	elif answers == 6:
		print "Unknown at this tine"
		
	elif answers == 7:
		print "Uncertain"
		
	elif answers == 8:
		print "Definitly not"  