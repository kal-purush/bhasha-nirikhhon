import os, sys

output_directory = 'recommendation_output'

if (not os.path.exists(output_directory)):
	os.system('mkdir ' + output_directory)

if (not os.path.exists('rec.txt')):
	print "Could not find output file rec.txt."
else:
	counter = 1
	while (os.path.exists(output_directory + '/rec' + str(counter) + '.txt')):
		counter += 1
	os.system('mv rec.txt ' + output_directory + '/rec' + str(counter) + '.txt')