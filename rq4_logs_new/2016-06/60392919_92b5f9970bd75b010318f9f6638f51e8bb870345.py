import time
import os

allCards = []
playHistory = []

class card:
    def __init__(self, cardID = -1, name = "", attack = -1, health = -1, pos = -1, zone = ""):
        self.id = cardID
        self.name = name
        self.attack = attack
        self.health = health
        self.position = pos
        self.zone = zone
    def setPos(self,pos):
        self.position = pos

class sifter:
    def __init__(self):
        self.directory = "C:\Program Files (x86)\Hearthstone"
        try:
            self.zoneLog = open(self.directory + "\\Logs\\Zone.log",'w')
            self.powerLog = open(self.directory + "\\Logs\\Power.log",'w')
            self.zoneLog.close()
            self.powerLog.close()
            self.zoneLog = open(self.directory + "\\Logs\\Zone.log",'r')
            self.powerLog = open(self.directory + "\\Logs\\Power.log",'r')
        except:
            print "Hearthstone is already running...shuffling to the end of the log."
            self.zoneLog = open(self.directory + "\\Logs\\Zone.log",'r')
            self.powerLog = open(self.directory + "\\Logs\\Power.log",'r')
            self.zoneLog.seek(0,2)
            self.powerLog.seek(0,2)
        self.zoneLine = ""
        self.powerLine = ""
        self.line = ""
    def sift(self):
        if(self.zoneLine == ""):
            self.zoneLine = self.zoneLog.readline()
        if(self.powerLine == ""):
            self.powerLine = self.powerLog.readline()
        if not self.zoneLine and not self.powerLine:
            time.sleep(.1)
            return -1
        elif not self.zoneLine:
            self.line = self.zoneLine
            self.zoneLine = ""
        elif not self.powerLine:
            self.line = self.powerLine
            self.powerLine = ""
        else:
            indexStart = self.zoneLine.find("D ") + 2
            indexEnd = self.zoneLine.find(" ",indexStart)
            zoneTime = self.zoneLine[indexStart:indexEnd]
            indexStart = self.powerLine.find("D ") + 2
            indexEnd = self.powerLine.find(" ",indexStart)
            powerTime = self.powerLine[indexStart:indexEnd]
            zoneTimeS1 = zoneTime.replace(":","")
            zonePowerS1 = powerTime.replace(":","")
            zoneTimeFormatted = zoneTimeS1.replace(".","")
            zonePowerFormatted = zonePowerS1.replace(".","")
            zoneTimeInt = int(zoneTimeFormatted)
            zonePowerInt = int(zonePowerFormatted)
            if(zoneTimeInt < zonePowerInt):
                self.line = self.zoneLine
                self.zoneLine = ""
            elif(zoneTimeInt > zonePowerInt):
                self.line = self.powerLine
                self.powerLine = ""
            else:
                self.line = self.zoneLine

def parseData(line):
    dataChanged = False
    if (line.find("TRANSITIONING") != -1):
        dataChanged = True
        if(line.find(" to FRIENDLY DECK") != -1):
            if(findCard(getCardID(line)) == -1):
                makeCard(getCardID(line),getCardName(line),"deckF")
            else:
                updateCard(getCardID(line),getCardName(line),-1,-1,"deckF",-1)
            playHistory.insert(0,"To Friendly Deck: [" + getCardID(line) + "] " + getCardName(line))
        elif(line.find(" to FRIENDLY HAND") != -1):
            if(findCard(getCardID(line)) == -1):
                makeCard(getCardID(line),getCardName(line),"handF")
            else:
                updateCard(getCardID(line),getCardName(line),-1,-1,"handF",-1)
            playHistory.insert(0,"To Friendly Hand: [" + getCardID(line) + "] " + getCardName(line))
        elif(line.find(" to OPPOSING DECK") != -1):
            if(findCard(getCardID(line)) == -1):
                makeCard(getCardID(line),getCardName(line),"deckO")
            else:
                updateCard(getCardID(line),getCardName(line),-1,-1,"deckO",-1)
            playHistory.insert(0,"To Opposing Deck: [" + getCardID(line) + "] " + getCardName(line))
        elif(line.find(" to OPPOSING HAND") != -1):
            if(findCard(getCardID(line)) == -1):
                makeCard(getCardID(line),getCardName(line),"handO")
            else:
                updateCard(getCardID(line),getCardName(line),-1,-1,"handO",-1)
            playHistory.insert(0,"To Opposing Hand: [" + getCardID(line) + "] " + getCardName(line))
        elif(line.find(" to FRIENDLY GRAVEYARD") != -1):
            if(findCard(getCardID(line)) == -1):
                makeCard(getCardID(line),getCardName(line),"graveF")
            else:
                updateCard(getCardID(line),getCardName(line),-1,-1,"graveF",-1)
            playHistory.insert(0,"To Friendly Graveyard: [" + getCardID(line) + "] " + getCardName(line))
        elif(line.find(" to OPPOSING GRAVEYARD") != -1):
            if(findCard(getCardID(line)) == -1):
                makeCard(getCardID(line),getCardName(line),"graveO")
            else:
                updateCard(getCardID(line),getCardName(line),-1,-1,"graveO",-1)
            playHistory.insert(0,"To Opposing Graveyard: [" + getCardID(line) + "] " + getCardName(line))
        elif(line.find(" to FRIENDLY PLAY") != -1):
            if(line.find("(Hero)") != -1):
                print "Friendly Hero: [" + getCardID(line) + "] " + getCardName(line)
            elif(line.find("(Hero Power)") != -1):
                print "Friendly Hero Power: [" + getCardID(line) + "] " + getCardName(line)
            else:
                if(findCard(getCardID(line)) == -1):
                    makeCard(getCardID(line),getCardName(line),"playF")
                else:
                    updateCard(getCardID(line),getCardName(line),-1,-1,"playF",-1)
                playHistory.insert(0,"To Friendly Play: [" + getCardID(line) + "] " + getCardName(line))
        elif(line.find(" to OPPOSING PLAY") != -1):
            if(line.find("(Hero)") != -1):
                print "Opposing Hero: [" + getCardID(line) + "] " + getCardName(line)
            elif(line.find("(Hero Power)") != -1):
                print "Opposing Hero Power: [" + getCardID(line) + "] " + getCardName(line)
            else:
                if(findCard(getCardID(line)) == -1):
                    makeCard(getCardID(line),getCardName(line),"playO")
                else:
                    updateCard(getCardID(line),getCardName(line),-1,-1,"playO",getCardZonePos(line))
                playHistory.insert(0,"To Opposing Play: [" + getCardID(line) + "] " + getCardName(line))
    elif(line.find("->") != -1 and line.find("FRIENDLY HAND") == -1 and line.find("OPPOSING HAND") == -1 and line.find("FRIENDLY PLAY") == -1 and line.find("OPPOSING PLAY") == -1 and line.find("FRIENDLY DECK") == -1 and line.find("OPPOSING DECK") == -1 and line.find("FRIENDLY GRAVEYARD") == -1 and line.find("OPPOSING GRAVEYARD") == -1):
        dataChanged = True
        cardID = getCardID(line)
        if(cardID != -1):
            pos = getCardZonePos(line)
            if(pos != -1):
                for card in allCards:
                    if(card.id == cardID):
                        card.position = pos
    #print dataChanged
    if(dataChanged):
        prettyData()
                                                        
def getCardID(line):
    indexLimit = line.find("[name=")
    if(indexLimit != -1):
        indexStart = line.find("id=",indexLimit)
    else:
        indexStart = line.find("id=")
    indexEnd = line.find(" ",indexStart)
    if(indexStart != -1 or indexEnd != -1):
        return line[indexStart+3:indexEnd]
    else:
        return "UNKNOWN"

def getCardName(line):
    indexStart = line.find("[name=")
    indexEnd = line.find("id=")
    if(indexStart != -1 and indexEnd != -1):
        return line[indexStart+6:indexEnd-1]
    else:
        return "UNKNOWN"

def getCardZonePos(line):
    indexStart = line.find("->")
    if(indexStart != -1):
        return line[indexStart+2:].strip()
    else:
        return -1

def findCard(cardID):
    for i in range(len(allCards)):
        if(allCards[i].id == cardID):
            return i
    return -1

def makeCard(cardID,name,zone):
    temp = card(cardID,name,-1,-1,-1,zone)
    allCards.append(temp)

def updateCard(cardID,name,atk,hp,zone,pos):
    for card in allCards:
        if(card.id == cardID):
            card.name = name
            card.attack = atk
            card.health = hp
            card.zone = zone
            card.position = pos

def prettyData():
    os.system('cls' if os.name == 'nt' else 'clear')    
    deckFcards = countCards("deckF")
    graveFcards = countCards("graveF")
    handFcards = countCards("handF")
    playFcards = countCards("playF")
    deckOcards = countCards("deckO")
    graveOcards = countCards("graveO")
    handOcards = countCards("handO")
    playOcards = countCards("playO")
    print "Friendly Graveyard:"
    for key, value in graveFcards.iteritems():
        print "    " + key + " x" + str(value)
    print "Friendly Deck:"
    for key, value in deckFcards.iteritems():
        print "    " + key + " x" + str(value)
    print "Friendly Hand:"
    for key, value in handFcards.iteritems():
        print "    " + key + " x" + str(value)
    print "Friendly Play:"
    for key, value in playFcards.iteritems():
        print "    " + key + " x" + str(value)
    print "Opposing Play:"
    for key, value in playOcards.iteritems():
        print "    " + key + " x" + str(value)
    print "Opposing Hand:"
    for key, value in handOcards.iteritems():
        print "    " + key + " x" + str(value)
    print "Opposing Deck:"
    for key, value in deckOcards.iteritems():
        print "    " + key + " x" + str(value)
    print "Opposing Graveyard:"
    for key, value in graveOcards.iteritems():
        print "    " + key + " x" + str(value)
    print "========================================"
    for i in range(0,len(playHistory)):
        if(i < 10):
            print playHistory[i]
    
def countCards(zone):
    uniqueCards = {}
    for card in allCards:
        if(card.zone == zone):
            try:
                uniqueCards[card.name] = uniqueCards[card.name] + 1
            except:
                uniqueCards.update({card.name:1})
    return uniqueCards

#Main Program Loop
logSifter = sifter()
while True:
    logSifter.sift()
    if(logSifter.line != -1):
        parseData(logSifter.line)
    