data = open('data.txt', 'r').readlines()
score = 0
for i in enumerate(data):
	theirPlay = i[1].rstrip('\n')[0]
	myPlay = i[1].rstrip('\n')[2]
	if theirPlay == 'A' and myPlay == 'X':
		# score += 4
		score += 3
	elif theirPlay == 'A' and myPlay == 'Y':
		# score += 8
		score += 4
	elif theirPlay == 'A' and myPlay == 'Z':
		# score += 3
		score += 8
	elif theirPlay == 'B' and myPlay == 'X':
		score += 1
	elif theirPlay == 'B' and myPlay == 'Y':
		score += 5
	elif theirPlay == 'B' and myPlay == 'Z':
		score += 9
	elif theirPlay == 'C' and myPlay == 'X':
		# score += 7
		score += 2
	elif theirPlay == 'C' and myPlay == 'Y':
		# score += 2
		score += 6
	elif theirPlay == 'C' and myPlay == 'Z':
		score += 7
print(score)