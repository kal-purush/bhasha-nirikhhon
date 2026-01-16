#############################
#   Edit these variables    #
#############################
group = 'version'
place = 'DVD'
thing = 'distro'
indexfile = '/home/daniel/distros'
#####################################################
#   Don't edit, unless you know what you are doing  #
#####################################################
import sys
index = open(indexfile,'r+')
def listall():
    for line in index:
        displayline(line)
def displayline(line):
    linelist=line.split('^')
    print(linelist[0] + " with " + group + ' ' + linelist[1] + " is on " + place + ' ' + linelist[2])
def listthing():
    thing_tmp = input("Which " + thing + " are you looking for?   ")
    for line in index:
        if thing_tmp in line:
            displayline(line)
def listplace():
    place_tmp = input("Which " + place + " are you looking for?   ")
    for line in index:
        if place_tmp in line:
            displayline(line)
def listgroup():
    group_tmp = input("Which " + group + " are you looking for?   ")
    for line in index:
        if group_tmp in line:
            displayline(line)
def add():
    thing_tmp = input("Which " + thing + " are you adding?   ")
    place_tmp = input("Which " + place + " is it on?  ")
    group_tmp = input("Which " + group + " is it (by)?  ")
    index.write(thing_tmp + "^" + place_tmp + "^" + group_tmp)
def delete():
    thing_tmp = input("Which " + thing + " are you deleting?   ")
    place_tmp = input("Which " + place + " is it on?   ")
    group_tmp = input("Which " + group + " is it (by)?   ")
    lines_to_delete = []
    for line in index:
        if thing_tmp in line and place_tmp in line and group_tmp in line:
            displayline(line)
            lines_to_delete.append(line)
        response = input("Are you SURE you want to delete these lines?   ").upper()
        response = response[:1]
        if response == "Y":
            lines = index.readlines()
            index.seek(0)
            for i in lines:
                if i not in lines_to_delete:
                    index.write(i)
def reset():
    response = input("Are you SURE you want to wipe the database?   ").upper()
    response = response[:1]
    if response == "Y":
        index.seek(0)
        index.write('')
def mainprogram():
    while True:
        print("What type of operation do you want to do?")
        print("1: read-only operations")
        print("2: edit the database")
        print("q: quit")
        choice=input()
        if choice == '1':
            ro()
        elif choice == '2':
            rw()
        elif choice == 'q':
            sys.exit()
        else:
            print("Input 1 or 2!")
def ro():
    while True:
        print("Which operation do you want to do?")
        print("1: list all " + thing +"s")
        print("2: find information about a/an " + thing)
        print("3: list everything with/by a specific " + group)
        print("4: list everything on a " + place)
        print("q: quit")
        choice=input()
        if choice == '1':
            listall()
        elif choice == '2':
            listthing()
        elif choice == '3':
            listgroup()
        elif choice == '4':
            listplace()
        elif choice == 'q':
            break
        else:
            print("Input 1-4!")
def rw():
    while True:
        print("Which operation do you want to do?")
        print("1: add a " + thing)
        print("2: remove a " + thing)
        print("3: wipe the database")
        print("q: quit")
        choice=input()
        if choice == '1':
            add()
        elif choice == '2':
            delete()
        elif choice == '3':
            reset()
        elif choice == 'q':
            break
        else:
            print("Input 1-3!")
mainprogram()