fname = raw_input('Please enter a file name: ')

try:
    fhand = open(fname)
except:
    print 'This file may be not in this directory.'
    quit()

for line in fhand:
    line.upper()
    print line