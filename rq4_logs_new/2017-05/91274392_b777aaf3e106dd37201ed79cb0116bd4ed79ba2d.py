import pandas
import numpy

# 2. The dataset "cdbrfss1999.csv" contains 159989 entries.
#     2.1. Take a sample of 20000 from this dataset and export it to an ASCII file. Make sure that your method
#     allows to draw more than one sample from the population.  [10 points]
#
#     2.2.  Discuss your method to do so. Is your sampling a "good sample" in the sense that it is representative for
#     the larger "population"?  [5 points]

# to run as module if imported otherwise as script
# def function():
#     print("This is a module function")
#
# if __name__=="__main__":
#     print("This is a script")

# with open('cdbrfss1999.csv') as csvImport:                              # opens file (, 'rb' -> r=read b=binary)
csvImport = "cdbrfss1999.csv"
rows = sum(1 for row in open(csvImport))                                # counts lines
import_size = 20000                                                     # sample size
# skip = sorted(random.sample(range(1, rows + 1), rows - import_size))    # creates a list of skipped lines (without header)

# random generator based on random function of numpy
rand_sample = [0]
rand_counter = import_size
while rand_counter > 0:
# for i in range(import_size):
    rand_nr = numpy.random.random_integers( 1, rows )
    if rand_nr in rand_sample:
        continue
    else:
        rand_sample.append( rand_nr )
        rand_counter -= 1
choose_rows = sorted( rand_sample )
print( 'chosen rows: %s' % choose_rows )

lines = pandas.read_csv( open(csvImport), skiprows = lambda x: x not in choose_rows )                 # StringIO(csvImport)
print( lines )
# print( type(lines) )
# sample_out = open( 'sample.txt', 'w' )
lines.to_csv( 'sample.csv', header=False, index=False )
print( "csv sample.csv created \n TASK 2 FINISHED" )