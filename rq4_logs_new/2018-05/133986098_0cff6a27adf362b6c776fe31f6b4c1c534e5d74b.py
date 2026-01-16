# CheckForBadCoordinatesInInputCSV.py
# Created by RachelG-GIS on GitHub
# Provided free of charge
# No support is guaranteed
# Tested in Python 2.7.14

# Open the CSV file for reading:

csvFile = open(r"<PathToYourFileWithSingleBackSlashes>","r")

# Read the lines. This splits the rows up into a list and counts the number of coordinates. If the count is even, nothing is printed. If it is odd, the script prints a statement for the user.
# The following lines:
# - Pull out the extra enter at the end of the line
# - Split the row into a list, using commas as the delineation since it's a CSV.
# - Counts the number of coordinates (number of columns, minus the polygon identifier column, divided by 2 (one X column and one Y column per vertex)
# - Advises the user if there's an odd number of columns, which would cause problems when importing the coordinates (i.e., you have a vertex with an X and not a Y)

for row in csvFile:
    inputRow = row.rstrip("\n")
    splitRow = inputRow.split(",")
    numberOfPoints = ((len(splitRow)-1)/2)-1 #
    currentPolygon = splitRow[0]
    if (len(splitRow)-1)%2 == 0:
        continue
    else:
        print currentPolygon+" has an odd number of coordinate columns and needs to be fixed or removed from the dataset."

# Close the file to ensure you're not leaving a lock on the file. If the script ends abruptly, be sure to run this line of code to close the file again:

csvFile.close()