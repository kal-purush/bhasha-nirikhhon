list = []

def showHelp():
  print("Seperate each item with a comma.")
  print("Type DONE to quit, SHOW to see the current list, and help to get this message.")

def showList():
  count = 1
  for item in list: 
    print("{}: {}".format(count, item))
    count += 1

print("Give me a list of things you want to shop for.")
showHelp()

while True:
  newStuff = input("> ")

  if newStuff == "DONE":
    print("Here's your list: ")
    showList()
    break
  elif newStuff == "HELP":
    showHelp()
    continue
  elif newStuff == "SHOW":
    showList()
    continue
  elif newStuff == "REMOVE":
    showList()
    itemToBeRemoved = input("What is the name of the item which you would like to remove? ")

    if itemToBeRemoved not in list:
      print("This is not in the list?")
    else:
      print("Removed {}".format(itemToBeRemoved))
      list.remove(itemToBeRemoved)
    continue
  else: 
    if newStuff == "":
        newStuff = input("You did not input an item, try again. \n> ")
        continue
    else: 
      newList = newStuff.split(",")
      index = input("Add this at a certain spot? Press enter for the end of the list, " "or give me a number. If you do not want to see this message again type 'cancel' Currently {} items in the list. ".format(len(list)))
    if index == "cancel":
      cancelConfirmation = input("Are you sure you would like to cancel this message? [Y/N] ")
      if cancelConfirmation == "N":
        break
      elif cancelConfirmation == "Y":
        print("Cancelling!")
        continue
    elif index:
      spot = int(index) - 1
      for item in newList:
        list.insert(spot, item.strip())
        spot += 1

    else: 

      for item in newList:
        list.append(item.strip())

  