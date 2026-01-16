# add delete clear list exit help run

import sys
import ping
import threading
import time

pinglist = []

def add():
    addin = input("What do you want to add?")
    if addin == "-back" or addin == "-b":
        print("Going back")
        return
    if addin not in pinglist and addin != "-back" and addin != "-b":
        pinglist.append(addin)

def addmore():
    addin = input("What do you want to add?")
    if addin == "-back" or addin == "-b":
        print("Going back")
        return
    if addin not in pinglist and addin != "-back" and addin != "-b":
        pinglist.append(addin)
    addmore()

def delete():
    delin = input("What do you want to delete?")
    if delin in pinglist:
        pinglist.remove(delin)
    elif delin == "-back" or delin == "-b":
        print("Going back")
    else:
        print("Item not in list")


def clear():
    yn = input("Are you sure you want to clear the list Y/N?")
    if yn == "y" or yn == "yes":
        del pinglist[:]
        print("List cleared")
    elif yn == "n" or yn == "no":
        print("List not cleared")
    elif yn == "-back" or yn == "-b":
        print("Going back")
    else:
        print("Wrong input")
        clear()


def list():
    if len(pinglist) <= 0:
        print("List empty")
        return
    pinglist.sort()
    for pl in pinglist:
        print(pl)

def exit():
    yn = input("Are you sure you want to exit Y/N?")
    if yn == "y" or yn == "yes":
        print("Exiting PingPython...")
        sys.exit()
    elif yn == "n" or yn == "no" or yn == "-back" or yn == "-b":
        print("Going back")
    else:
        print("Wrong input")
        exit()

def help():
    str = "MANUAL for PingPython by Brob"
    print(str.center(80, ' '), "\n"
          "---------------------------------------------------------------------------------------------------\n"
          "Add\t\t-\tAdd anything to the list and it will ping it when the run command is given. \n"
                               "add, a\n\n"
          "AddMore\t-\tEnters the add mode until -back or -b is typed in\n"
                               "addmore, am\n\n"
          "Delete\t-\tEnter what you would like to delete from the list.\n"
                               "delete, del, d\n\n"

          "Clear\t-\tThis command will clear the entire list\n"
                               "clear, clr, c\n\n"

          "List\t-\tPrints out the complete list of ping entities\n"
                               "list, lst, l\n\n"

          "Run\t\t-\tThis will run the ping loop and notify when getting a 'timeout' or a long ping delay\n"
                               "run, r\n\n"

          "Help\t-\tPrints this manual\n"
                               "help, h\n \n"

          "Exit\t-\tExits the program\n"
          "exit, e, quit, q\n\n"
          "-back or -b will bring you to the start of the program.\n"
          "---------------------------------------------------------------------------------------------------\n")

def run():
    if len(pinglist) <= 0:
        print("There is nothing to print")
        return
    print("Quit to main menu by typing quit, q, back or b")


    def doit():
        t = threading.currentThread()
        i = 0
        while getattr(t, "do_run", True):
            if not ping.ping(pinglist[i]):
                print(pinglist[i], " not up")
            time.sleep(1)
            if i == len(pinglist)-1:
                i = 0
            else:
                i += 1
        print("Going back")

    thread = threading.Thread(target=doit)
    thread.start()
    time.sleep(len(pinglist))

    runshit = True
    while runshit:
        inn =input()
        if inn == "quit" or inn == "q" or inn == "back" or inn == "b":
            thread.do_run = False
            thread.join()
            runshit = False


