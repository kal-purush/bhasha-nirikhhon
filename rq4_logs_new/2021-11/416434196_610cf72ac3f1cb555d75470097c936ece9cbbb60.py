#Compare the netcdf files among all corresponding subdirectories of the
# the two directories using GFDL's (Remiks) nccmp utility
def comp_with_nccmp_subdirs (d1, d2):
    subdirs =  [f.name for f in os.scandir(d1) if f.is_dir()]
    print("Subdirectories to compare:")
    for element in subdirs:
        print (element)

    print("-----------------------------")
    global files_failed
    global files_compared
    files_failed = 0
    files_compared = 0
    for element in subdirs:
        comp_with_nccmp(d1 + '/' + element, d2 + '/' + element)

    print("[total files compared, total files differing]: [" + str(files_compared) + ", " + str(files_failed) +"]")
    print("-----------------------------")

#Compare the netcdf files among all corresponding subdirectories of the
# the two directories using GFDL's (Remiks) nccmp utility
def comp_with_nccmp_subdirs (d1, d2):
    subdirs =  [f.name for f in os.scandir(d1) if f.is_dir()]
    print("Subdirectories to compare:")
    for element in subdirs:
        print (element)

    print("-----------------------------")
    global files_failed
    global files_compared
    files_failed = 0
    files_compared = 0
    for element in subdirs:
        comp_with_nccmp(d1 + '/' + element, d2 + '/' + element)

    print("[total files compared, total files differing]: [" + str(files_compared) + ", " + str(files_failed) +"]")

if __name__ == '__main__':

    if (len(sys.argv) != 3):
        print('Usage: dircompnc.py director1 directory 2')
        sys.exit(-1)

    dirA = sys.argv[1]
    dirB = sys.argv[2]
    print('Directory A: ', dirA)
    print('Direcotry B: ', dirB)
    if (path.exists(dirA) == False):
        print(dirA, 'does not exist...bye')
        sys.exit(-1)
    if (path.exists(dirB) == False):
        print(dirB, 'does not exist...bye')
        sys.exit(-1)
    if (path.isdir(dirA) == False):
        print(dirA, 'is not a directory. ... bye ...')
        sys.exit(-1)
    if (path.isdir(dirB) == False):
        print(dirB, 'is not a directory. ... bye ...')
        sys.exit(-1)

    comp_with_fatts(dirA, dirB)

    #comp_with_nccmp(dirA, dirB)
    comp_with_nccmp_subdirs(dirA, dirB)
    print("All comparisons fininshed")