import shutil
import os
import sys
from NodeManager import NodeManager

class FrontierManager():
	"""docstring for FrontierFile"""
	def __init__(self, root, goal, graphFile, heuristicFile):
		self.uid = 1
		rootName = str(0) + "_" + root + "_" + str(0)
		file = open("frontier/frontierFile", "w+")
		print rootName
		file.write(rootName + "\n")
		file.close()

		file2 = open("frontier/" + rootName, "w+")
		print "frontier/" + rootName
		file2.write(str(0) + "\n")
		file2.write(root)
		file2.close()

		self.goalFound = False
		self.goal = goal

		self.explore = False

		self.nodeManager = NodeManager()
		self.nodeManager.generateGraph(graphFile, heuristicFile)

	def expandPath(self, line, child, pathCost, heuristicCost):
		print "\t######expand path######"
		file = open("frontier/" + line, "r")

		totalCost = int(file.next()) + int(pathCost)
		completeCost = totalCost + heuristicCost

		split = line.split("_")

		print "\tfrontier/" + str(completeCost) + "_" + child + "_" + str(self.uid), "w+"

		newFile = open("frontier/" + str(completeCost) + "_" + child + "_" + str(self.uid), "w+")
		text = file.next()

		newFile.write(str(totalCost) + "\n")

		print "\tCost: " + str(totalCost)

		# Loop until EOF
		try:
			while True:
				print "\tPath: " + text
				newFile.write(text + "\n")		
				text = text[:-1]
				text = file.next()
		except StopIteration as e:
			pass

		newFile.write(child + "\n")

		print "\tPath: " + child

		self.uid = self.uid + 1

		file.close()
		newFile.close()

		# Copy frontier and rename, ready to append new node to path
		#newPath = "frontier/" + str(self.cost + cost) + self.name + str(self.uid)
		#shutil.copyfile(self.name, newPath)

	# Adds children, returns the number of children added
	def addFrontierFile(self, node):
		if node == -1:
			return -1

		print "######addFrontierFile########"
		file = open("frontier/frontierFile", "r")
		line = file.next()
		line = line[:-1]
		updated = open("frontier/updated", "w+")

		childrenCount = 0

		try:
			# Loop until EOF
			while True:
				print "\tline " + repr(str(line))
				print "\tnode " + str(node)
				if line == node:
					print "\tLine == node (found best in frontier file)"
					split = line.split("_")
					parent = self.nodeManager.getNode(split[1])

					for child in parent.getConnections():
						print "\tChild in parent.getConnections(): " + child

						#if self.explore == True and parent == self.goal

						if self.alreadyVisited(line, child):
							print "\tContinued"
							continue

						# Get path cost so far
						pathFile = open("frontier/" + line, "r")
						actualNode = self.nodeManager.getNode(child)

						#completeCost = int(file.next()) + pathCost + heuristicCost
						completeCost = int(pathFile.next()) + int(parent.getCost(child)) + int(actualNode.heuristic)
						pathFile.close()

						# Add file name to frontierFile
						updated.write(str(completeCost) + "_" + child + "_" + str(self.uid) + "\n")
						print "\tAppended to updated: " + str(completeCost) + "_" + child + "_" + str(self.uid)

						# Create frontier file
						self.expandPath(line, child, parent.getCost(child), self.nodeManager.getNode(child).heuristic)

						childrenCount = childrenCount + 1

						print "\tGoal: " + self.goal + " - Child: " + repr(child)
						if self.goal == child:
							self.goalFound = True

					os.remove("frontier/" + line)
					print "Removed: frontier/" + line
				else:
					# Write line to new file
					updated.write(line + "\n")

				line = file.next()
				print line
				#line = file.next()
				#print line + "dfg"
				line = line[:-1]
				#print line
		except StopIteration as e:
			print "ugh"

		file.close()
		updated.close()

		os.remove("frontier/frontierFile")
		shutil.copyfile("frontier/updated", "frontier/frontierFile")
		os.remove("frontier/updated")

		if self.goalFound:
			childrenCount = 0

			pathList = open("frontier/frontierFile", "r")		
			path = pathList.next()

			try:
				while True:
					self.printPaths(path[:-1])
					path = pathList.next()			
			except StopIteration as e:
				pass

			pathList.close()

		return childrenCount	

	# USES 1 NODE
	def alreadyVisited(self, line, nodeName):
		# Loop until EOF
		file = open("frontier/" + line, "r")
		line = file.next()
		#print "alreadyVisited 1 " + line[:-1]
		line = file.next()
		#print "alreadyVisited 2 " + line
		
		try:
			while True:
				#print "Line: " + line
				#print "NodeName: " + nodeName
				if line[:-1] == nodeName:
					file.close()
					print "\t\t" + line + "ALREADY VISITED"
					return True

				line = file.next()
		except StopIteration as e:
			pass

		file.close()

		print "\t\t" + line + " NOT VISITED"

		return False

	# USES 1 NODE first
	def findBestNode(self):
		print "#######Find best node######"
		file = open("frontier/frontierFile", "r+")
		
		line = ""

		try:
			line = file.next()
			#print "\tfrontierFile: " + line[:-1]
		except StopIteration as e:
			pass

		info = line.split("_")

		# Default the first value to best for comparison

		bestCost = info[0]
		bestName = info[1]
		bestID = info[2]

		bestOption = line

		try:
			# Loop until EOF
			while True:
				info = line.split("_")

				if info[0] == bestCost:
					if info[1] < bestName:
						bestCost = info[0]
						bestName = info[1]
						bestID = info[2]
						bestOption = line
				elif info[0] < bestCost:
						bestCost = info[0]
						bestName = info[1]
						bestID = info[2]
						bestOption = line

				print "\tfrontierFile: " + line[:-1]
				line = file.next()
		except StopIteration as e:
			pass

		file.close()

		print "\tBest Option: " + bestOption[:-1]

		return bestOption[:-1]

	def printPaths(self, file):
		nodes = open("frontier/" + file, "r")
		line = nodes.next()
		line = nodes.next()
		string = ""
		try:
			while True:
				rep = repr(line[:-1])
				if rep != "''":
					string = string + repr(line[:-1])
					
				line = nodes.next()

				if rep != "''":
					string = string + " --> "
		except StopIteration as e:
			nodes.close()
			print string

	def findAlternatives(self, limit):
		self.goalFound = False
		self.removeGoals()
		print "Find Alternatives"
		file = open("frontier/frontierFile", "r+")
		
		line = ""

		try:
			line = file.next()
			print "1"
		except StopIteration as e:
			pass
		
		info = line.split("_")

		# Default the first value to best for comparison

		bestCost = info[0]
		bestName = info[1]
		bestID = info[2]

		bestOption = line

		try:
			# Loop until EOF
			while True:
				print "2"
				info = line.split("_")

				if info[0] == bestCost:
					if info[1] < bestName:
						bestCost = info[0]
						bestName = info[1]
						bestID = info[2]
						bestOption = line
				elif info[0] < bestCost:
						bestCost = info[0]
						bestName = info[1]
						bestID = info[2]
						bestOption = line

				line = file.next()
		except StopIteration as e:
			pass

		file.close()

		if bestCost > limit:
			print "Nothing better can be found"
			return -1

		print "Best Option: " + bestOption[:-1]

		return bestOption[:-1]

	def removeGoals(self):
		pathList = open("frontier/frontierFile", "r")
		updated = open("frontier/updated", "w+")	
		path = pathList.next()

		try:
			while True:
				info = path.split("_")
				if self.goal == info[1]:
					os.remove("frontier/" + path[:-1])
					print "Removed Goal: " + path
				else:
					updated.write(path)

				path = pathList.next()			
		except StopIteration as e:
			updated.close()
			pathList.close()

		os.remove("frontier/frontierFile")
		shutil.copyfile("frontier/updated", "frontier/frontierFile")
		os.remove("frontier/updated")