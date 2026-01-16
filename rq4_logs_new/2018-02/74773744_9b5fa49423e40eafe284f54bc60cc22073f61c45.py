in_file = open('occurrences.txt', 'r')
out_file = open('present.txt', 'w')

counter = 7740

for line in in_file:
	row = line.split('\t')
	country = row[5]
	id = row[0]
	ref = row[8]
	if country == '':
		country = row[6]
	if country == '':
		pass
	else:
		out_file.write('M' + str(counter) + '\t' + id + '\t\ttrue\t' + 'http://eol.org/schema/terms/Present' + '\t' + country + '\t\t\t\tCompiler: Anne E Thessen\t' + ref + '\n') 
		counter = counter + 1