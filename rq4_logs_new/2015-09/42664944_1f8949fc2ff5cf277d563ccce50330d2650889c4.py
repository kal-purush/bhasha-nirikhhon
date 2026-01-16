'''
snakes and ladders
possible classes: dice,turn,player(incl score and turn)
'''

import random
import sys

def unused():
    pass
        
    #def setPlayers(self, playerCount = 0):
    #    ''' Creates the players.
        
    #    Keyword args:
    #    playerCount -- (default: 0, -- User Input) Number of Players.

    #    Return: Dict
    #    '''
    #    self.playerList = {}
    #    if playerCount != 0:
    #        self.playerCount = playerCount
    #    else:
    #        self.playerCount = int(input('How many are playing? '))
    #    for idx in range(1,self.playerCount+1): 
    #        print('Enter name of player ', idx, ':')
    #        playerNameTemp = input()
    #        self.playerList[idx] = playerNameTemp
    #    return(self.playerList)
    
 

    #def playerTokens(self):
    #    playerTokenPosition = {}
    #    for idx in range(1,self.maxTokens+1):
    #        playerTokenPosition = {idx,playerTokenPosition}


    #class Board(object):
    #    ''' #Specifies the attributes of the board.
    #    '''

    #    boardLayout = {1:38, 
    #                   4:14, 
    #                   9:31, 
    #                   17:7, 
    #                   21:42, 
    #                   28:84, 
    #                   51:67, 
    #                   54:34, 
    #                   62:19, 
    #                   64:60, 
    #                   71:91,
    #                   80:100, 
    #                   87:24, 
    #                   93:73, 
    #                   95:75, 
    #                   98:79}


    



    #def startCheck(rolledNumbers,gateValue=[6]):
    #        ''' Checks if the player has a rolled the gating values before they can move their token.

    #        Keyword args:
    #        rolledNumbers -- accepts the numbers rolled by the dice in a player's turn.
    #        gateValue -- specifies the value(s) the player needs to roll to begin play. Default is 6.

    #        Returns:boolean
    #        '''

    #        playerOpen = rolledNumbers in gateValue
    #        return(playerOpen)
        



    #rolledNums = rollDice(getDice())
    #print(rolledNums)
    #print(startCheck(rolledNums))

    #gameKeeper = GameKeeper()
    #print(gameKeeper.setPlayers())

    #dice = Dice(customDice=0,diceCount=1, diceSides=6)
    #print(startCheck((dice.rollDice())))

    #MAXPOSITION = 100
    #createDice(sides,number)
    #playerCount = input('Enter number of players: ')
    #for loopCounter in range(1,playerCount):
    #    print('Enter player %d name:', loopCounter)
    #    playerNameTemp = input()
    #    setPlayers(playerNameTemp,playerPosition=0)

    #for turnCounter in range(1,MAXPOSITION):
    #    for playerCounter in range(1,playerCount):    
    #        getPlayer(playerCount)
    #        newPosition = rollDie()
    #        movePlayer(checkBoard(newPosition))
    pass

class Dice(object):
    ''' Specifies the attributes required for the dice.

        Keyword args:
            customDice -- (default: 0) Determines if player can input the dice parameters. 0 -- No, 1 -- Yes.
            diceCount -- (default: 1) Sets number of dice used in the game. Range: int([1: )
            diceSides -- (default: 6) Sets the number of sides each dice has. Range: int([1: )

    '''

    def __init__(self,customDice=0, diceCount=1, diceSides=6):
        ''' Initializes the parameters of the dice- how many dice, how many sides.

        returns: none
        '''

        if customDice != 0 :
            diceCount = input('How many dice can the game use? (Default is 1): ')
            diceSides = input('How many sides does each dice have? (Default is 6): ')

        
        try:
            #abs(int(float(diceCount): ensures any value entered is converted to appropriate number.
            #Ex - float('-6.7') --> -6.7, int(-6.7) --> -6, abs(-6) --> 6.

            diceCount = abs(int(float(diceCount)))
        except ValueError:
            # raised if user input can not be converted into a numerical value. Ex- 'a' .
            print('Invalid value. Number of dice can only be an integer with value 1 or more. Using default value of 1.')
            diceCount = 1
        except NameError:
            # raised if user input is a character which the program deals with as a variable. Ex- a .
            print('Invalid value. Number of dice can only be an integer with value 1 or more. Using default value of 1.')
            diceCount = 1
        try:
            diceSides = abs(int(float(diceSides)))
        except ValueError:
            print('Invalid value. Number of sides a dice has can only be an integer with value 1 or more. Using default value of 6.')
            diceSides = 6
        except NameError:
            print('Invalid value. Number of sides a dice has can only be an integer with value 1 or more. Using default value of 6.')
            diceSides = 6
        self.diceCount = diceCount
        self.diceSides = diceSides
    
    def rollDice(self):
        '''Rolls the dice for one turn.

        Keyword args:
        diceSides -- number of sides of the dice. Default is 6.
        diceCount -- number of dice rolled together. Default is 1.

        Returns: list of int
        '''

        #Sampling 'diceCount' number of values from population 1:diceSides without replacement.
        # How well does it work for 2 dice? Maybe I should use sets for each die.
        rolledNumbers = random.sample(range(1,self.diceSides+1),self.diceCount)
        return(sum(rolledNumbers))

class GameMaster(object):
    ''' Setsup and runs the game.
    '''
    

    def __init__(self):
        
        self.Player = Player()
        self.Dice = Dice()

    def getPlayerCount(self):
        playerCount = input('Enter number of players: ')
        try:
            #abs(int(float(playerCount): ensures any value entered is converted to appropriate number.
            #Ex - float('-6.7') --> -6.7, int(-6.7) --> -6, abs(-6) --> 6.
            playerCount = abs(int(float(playerCount)))
        except ValueError:
            # raised if user input can not be converted into a numerical value. Ex- 'a' .
            print('Invalid value. Number of players can only be an integer with value 1 or more. Using default value of 1.')
            playerCount = 1
        except NameError:
            # raised if user input is a character which the program deals with as a variable. Ex- a .
            print('Invalid value. Number of players can only be an integer with value 1 or more. Using default value of 1.')
            playerCount = 1
        return(playerCount)

    def winCheck(self,currentPlayerOutcome,currentPlayerName):
        if currentPlayerOutcome == 1:
            print('Player',currentPlayerName,'won!')
            print('Press any key to end program.')
            input()
            sys.exit()

    def snlReposition(self,currentPlayerPosition):
        boardLayout = {1:38, 
                4:14, 
                9:31, 
                17:7, 
                21:42, 
                28:84, 
                51:67, 
                54:34, 
                62:19, 
                64:60, 
                71:91,
                80:100, 
                87:24, 
                93:73, 
                95:75, 
                98:79}
        
        try:
            return(boardLayout[currentPlayerPosition])
        except KeyError:
            return(currentPlayerPosition)
        

class Player(object):
    ''' Specifies attributes for the player class.
    '''

    def __init__(self,playerName = '',playerPosition=0):
        ''' Initializes parameters of the player.

        Keyword args:
        playerName -- (default: '') Specifies plaeyr name.
        playerPosition -- (default: 0) Specifies score of player. STarting score is 0.
        '''
        self.playerName = playerName
        self.playerPosition = playerPosition
        self.playerWon = 0
        self.Dice = Dice()
        
    def rollDice(self):
        return(self.Dice.rollDice())

    def getPlayer(self,playerQuery='position'):
        if(playerQuery == 'name' or playerQuery == 'Name' or playerQuery =='NAME'):
            return(self.playerName)
        elif(playerQuery == 'position' or playerQuery == 'Position' or playerQuery == 'POSITION'):
            return(self.playerPosition)    
        elif(playerQuery == 'outcome' or playerQuery == 'Outcome' or playerQuery == 'OUTCOME'):    
            return(self.playerWon)
      
    def movePlayer(self,newPosition):
        if(self.playerPosition == 0 and newPosition != 6):
            print('Player',self.playerName,'cannot move until they roll a 6. They rolled a', newPosition,'. Turn to next player.')
        elif(self.playerPosition == 0 and newPosition == 6):
            newPosition = Player.rollDice(self)
            self.playerPosition = newPosition
            print('We have a 6! And',self.playerName,'moves to', self.playerPosition,'.')
        else:
            self.playerPosition = newPosition
            if(self.playerPosition > MAXPOSITION):
                self.playerWon = 1
            else:
                print('',self.playerName,'moves to', self.playerPosition,'.')

        
if __name__ == '__main__':

    MAXPOSITION = (100)
    dice = Dice(customDice=0,diceCount=1,diceSides=6)
    game = GameMaster()
    playerCount = game.getPlayerCount()
    allPlayers = []
    for playerCounter in range(0,playerCount):
        print('Enter player',playerCounter+1,'name:')
        playerNameTemp = input()
        allPlayers.append(Player(playerName=playerNameTemp, playerPosition=0))


    for turnCounter in range(0,MAXPOSITION):
        for playerCounter in range(0,playerCount):
            rolledValue = dice.rollDice()
            newPosition = rolledValue + allPlayers[playerCounter].getPlayer('position')
            allPlayers[playerCounter].movePlayer(newPosition)
            currentPlayerPosition = allPlayers[playerCounter].getPlayer('position')
            rePosition = game.snlReposition(currentPlayerPosition)
            if rePosition > currentPlayerPosition:
                print('',allPlayers[playerCounter].getPlayer('name'),'found a ladder at',
                      currentPlayerPosition,'! Moving to',rePosition,'!')
                allPlayers[playerCounter].movePlayer(rePosition)
            elif rePosition < currentPlayerPosition:
                print('',allPlayers[playerCounter].getPlayer('name'),'was bitten by a snake at',
                      currentPlayerPosition,'! Moving to',rePosition,'!')
                allPlayers[playerCounter].movePlayer(rePosition)
            game.winCheck(allPlayers[playerCounter].getPlayer('outcome'),allPlayers[playerCounter].getPlayer('name'))


            


