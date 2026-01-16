# ConvertInputCSVToArcMapReadableFormat.py
# Created by RachelG-GIS on GitHub
# Provided free of charge
# No support is guaranteed
# Tested in Python 2.7.14

# Open the CSV file for reading:

csvFile = open(r"<PathToYourOldCSVFileWithSingleBackSlashes>","r")

# Create the new output CSV file and open it for writing:

newCSVFile = open(r"<PathToANewNonexistentOrEmptyCSVFileWithSingleBackSlashes>", "w+")

# Read the lines. This splits the rows up into a list and transposes the values into new lines in the new CSV.
# The following lines:
# - Pull out the extra enter at the end of the line
# - Split the row into a list, using commas as the delineation since it's a CSV.
# - Starts an x/y variable that swaps to indicate whether the inserted line should be followed by a comma or a line break
# - Inserts the new lines into the new CSV

for row in csvFile:
    inputRow = row.rstrip("\n")
    splitRow = inputRow.split(",")
    numberOfPoints = ((len(splitRow)-1)/2)-1
    currentPolygon = splitRow[0]
    currentCoordinate = "x"
    for value in splitRow[1:]:
        if currentCoordinate == "x":
            newCSVFile.write(currentPolygon+","+str(value)+",")
            currentCoordinate = "y"
            continue
        if currentCoordinate == "y":
            newCSVFile.write(str(value)+"\n")
            currentCoordinate = "x"
            continue

# Close the files to ensure you're not leaving a lock on the file. If the script ends abruptly, be sure to run this line of code to close the file again:

csvFile.close()
newCSVFile.close()

# Advise that the script is done:

print "Script is complete."