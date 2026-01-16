import sys
from prettytable import from_csv

# PrettyTable: Display data in tables using a beautiful ASCII format.
__author__ = 'Merrick Ryman'
# Thank you luke@maurits.id.au (code.google.com) for providing the prettytable library.
# TODO: Add sort functionality (ex. sort by URL or genre by alphabetical order)

print "Run 'addCollection' to append Collection(s) to the Collections CSV file.\n"
print "Displaying Collections... This may take awhile.\n"

collectionsData = open("Collections.csv", "r")
collectionsTable = from_csv(collectionsData)
print collectionsTable
collectionsData.close()
x = raw_input("Press enter to exit.")
sys.exit("Completed.")