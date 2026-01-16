# fuelEconomy.py
# Created By Jo Narvaez-Jensen

# This program takes two data files that contain car fuel economy numbers
#    (city mpg, hwy mpg), finds their averages, and determines the # of
#   inefficient cars based on either an mpg < 22 City and/or 27 Hwy.

# takes in the 4 data sets and prints them out
def printResults (carCount, avgCity, avgHwy, guzzlers):
    print ("The total number of cars tested: {0}.".format (carCount))
    print ("The total average of City Miles per Gallon: {0} MPG.". format (avgCity))
    print ("The total average of Highway Miles per Gallon: {0} MPG.". format (avgHwy))
    print ("The number of Gas Guzzlers (Critera: City MPG < 22 Miles and/or Highway MPG < 27 Miles) among the cars tested: {0}.". format (guzzlers))

# Determines the # of gas hungry cars using the critera of mpg that is less than 22 city and/or 27 hwy
def hungryCars (city, hwy):
    total = 0
    for i in range (len (city)):
        if float (city[i]) < 22 or float (hwy[i]) < 27:
            total = total + 1
    return total

# Determine a list's combined average
def calculateAvg (listOfAvg):
    sum = 0

    for i in listOfAvg:
        sum += float (i)
    finalAvg = round (sum / len (listOfAvg), 1)

    return finalAvg

# Creates a list from imported data
def fileConverter (fileData):
    splitList = fileData.read ().rstrip (). split ('\n')
    return splitList

# imports and returns 2 data sets from specified text files
def fileReader ():
    cityInfo = open ('carModelData_city.txt', 'r')
    hwyInfo = open ('carModelData_hwy.txt', 'r')
    return cityInfo, hwyInfo

def main ():
# Input
    cityDetails, hwyDetails = fileReader ()

# Processing
    # City Miles per Gallon evaluation
    cityMPG = fileConverter (cityDetails)
    cityAvg = calculateAvg (cityMPG)

    # Highway Miles per Gallon evaluation
    hwyMPG = fileConverter (hwyDetails)
    hwyAvg = calculateAvg (hwyMPG)

    #Count the guzzlers
    guzzlersCount = hungryCars (cityMPG, hwyMPG)

# Output
    printResults (len (cityMPG), cityAvg, hwyAvg, guzzlersCount)

main ()