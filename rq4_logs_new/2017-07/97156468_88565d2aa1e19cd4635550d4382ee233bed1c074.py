import random
playerWin = 0
computerWin = 0
winLoss = 0
play = "yes"
while (play == "yes"):
  player = input("Enter your choice: (rock/paper/scissors): ")
  player = player.lower()
  while (player != "rock" and player != "paper" and player != "scissors"):
    print player
    print "You big dummy... "
    player = input("Enter your choice: (rock/paper/scissors): ")
    player = player.lower()
  
  computerInt = random.randint(1,3)
  if (computerInt == 1):
    computer = "rock"
  elif (computerInt == 2):
    computer = "paper"
  elif (computerInt == 3):
    computer = "scissors"
    
  if (player == computer):
    winLoss = 1
  elif (player == "rock"):
    if (computer == "scissors"):
      winLoss = 2
      playerWin += 1
    if (computer == "paper"):
      winLoss = 0
      computerWin += 1
  elif (player == "paper"):
    if (computer == "rock"):
      winLoss = 2
      playerWin += 1
    if (computer == "scissors"):
      winLoss = 0
      computerWin += 1
  elif (player == "scissors"):
    if (computer == "paper"):
      winLoss = 2
      playerWin += 1
    if (computer == "rock"):
      winLoss = 0
      computerWin += 1
      
  print "\nYou chose " + player + ", and the computer chose " + computer + ".\n"
  if (winLoss == 0):
    print "Computer won... which means you lost..."
  elif (winLoss == 1):
    print "You tied with the computer... Congrats?"
  elif (winLoss == 2):
    print "You won!  Aren't you so smart!?!!"
  
  print "\n******SCOREBOARD******"
  print "\nYour Score: " + str(playerWin)
  print "\nComputer Score: " + str(computerWin)
  print "\n**********************"
  
  play = input("Would you like to play again? (yes/no)")
  
