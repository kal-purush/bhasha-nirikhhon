import csv
from collections import defaultdict
from prettytable import PrettyTable


def listFromcsv(filename):
	
	l = list()
	i = 0 # dr number - 1
	count = 0
	with open(filename, 'rb') as f:
		reader = csv.reader(f)
		for row in reader:
			if row[1]=="Name":
				continue

			if row[2]!="":
				count += 1
			
	print count,"students have filled yet"

	lister = [[] for i in range(count)] # gives list for each student who has filled priorities

	i = 0 
	with open(filename, 'rb') as f:
		reader = csv.reader(f)
		for row in reader:
			if row[1]=="Name":
				continue
			if i == count:
				break
			j=2
			lister[i].append(row[1])
			#print row[1],len(row)
			while(1):
				if row[j]=='' :# last choice
					break
				if row[j][0]=='/':# comments ignored
					break
				if row[j][0]==' ':# because there exist dumbf#cks
					break
				row[j] = row[j].strip()
				lister[i].append(row[j])
				# print row[j] # j-1 th choice
				j += 1

			i += 1

	# print lister
	return lister


def dicttFromcsv(filename):

	with open(filename, mode='rb') as infile:
		reader = csv.reader(infile)
		d = d = defaultdict(list)
		for row in reader:
			if row[1]=="Code":
				continue

			d[row[1]].append(int(row[7]))
			d[row[1]].append(row[8])
			d[row[1]].append(0)
			#d[row[1]]=list(int(row[7]),row[8],0)
			# row 1 is code , 7 is capacity , 8 is type of student
			# 0 is the filled value to be used later
	return d



def allot(projects,people,details):
	"""
		Print DR# wise project alloted
		details matlab print small details for allocation
	"""

	statsTable = PrettyTable(['Name','Alloted'])
	for student in people: # student has name and all choices of him/her
		
		if(details=="1"):
			print "\n",student[0],"\t\t",
			print student
		for i in range(1,len(student)):
			#print student[i]

			# student[i] is i-1 th priority
			if student[i] not in projects: 
				statsTable.add_row([student[0],"tumse na ho payega"])
				break
			thisProject = projects[student[i]]
			# again , trusting people fill proper codes

			if thisProject[2] == thisProject[0]: # capacity full
				
				# remove below for debugging
				
				if(details=="1"):
					print student[i],"full\t",
				if i==len(student)-1:
					if(details=="1"):
							print "Tumse na ho payega"
					statsTable.add_row([student[0],"tumse na ho payega"])
				continue # look for next 
		
			# if here , it means student gets it
			if(details=="1"):
				print student[i],"alloted"
			statsTable.add_row([student[0],student[i]])
			projects[student[i]][2] += 1 # update filled positions
			break
		
	# tabulated results are printed
	print statsTable

		

def main():
	"""
		Get shit done.
	"""

	details = raw_input("Print allocation details ? 1 for yes , any other char for no : ")
	projects = dicttFromcsv("Projects.csv")
	
	#assumed that Choices.csv is sorted by DR#
	# ignoring constraints like Any, BTech , Dual , CG > 8.5 for now
	# Expecting people not to be arseholes , like a dual guy taking btech specific
	people = listFromcsv("Choices.csv")

	#print len(people)
	allot(projects,people,details)


if __name__=="__main__":
	main()
