#CSCA20 Week 7 Notes
#Objectives:
    #1. Practice processing input from files
    #2. Use split() method to separate strings


def skip_header(f):
    '''Given an open file f, reads past its three-line header.

    Args:
        f: a freshly opened file in the format of the file cryer.dat
    '''
    lines = f.readline()
    lines = f.readline()
    lines = f.readline()

def avg_temp_march(f):
    '''Returns the average temperature for the month of March for all years
    with data in f.

    Args:
        f: a freshly opened file in the format of the file cryer.dat
    '''
    skip_headers(f)
    list_lines = f.readlines()
    for line in list_lines:
        line.split()