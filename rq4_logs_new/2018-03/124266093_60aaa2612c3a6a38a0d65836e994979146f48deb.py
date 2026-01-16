#IMPORTS:
from random import randint
import time


#STATS INDEX:
#0: Name             |   Ask
#1: Race             |   Ask
#2: Class            |   Ask
#3: HP               |   30 + x * 10 (Ex: HP++, x = 2)
#4: Max HP           |   30 + x * 10 (Ex: HP++, x = 2)
#5: HP Percentage    |   HP / Max HP * 100
#6: Weapon           |   Depends on class
#7: Minimum Damage   |   3 + x * 2 (Ex: DMG++, x = 2)
#8: Maximum Damage   |   5 + x * 2 (Ex: DMG++, x = 2)


#LISTS:
Races = ["human","elf","dwarf","orc","uruk-hai","goblin","tee"]
Classes = ["warrior","archer","mage","godmodegg"]
JungEnemies = ["Goblin","Boar"]
Actions = ["fight","rest","actions","stats","leave","contact"]


#GIVE BASIC STATS
hName = input("\nGreetings! What's your name?\n")
hRace = input("\nAnd your race?\n>Human\n>Elf\n>Dwarf\n>Orc\n>Uruk-hai\n>Goblin\n")
hRace = hRace.lower()
while hRace not in Races:
    hRace = input("\nChoose an existent race\n>Human\n>Elf\n>Dwarf\n>Orc\n>Uruk-hai\n>Goblin\n")
    hRace = hRace.lower()
hClass = input("\nOk, last question. What are you better as?\n>Warrior ( HP+++ | DMG++   )\n>Archer  ( HP++  | DMG+++  )\n>Mage    ( HP+   | DMG++++ )\n")
hClass = hClass.lower()
while hClass not in Classes:
    hClass = input("\nChoose an existent class\n>Warrior ( HP+++ | DMG++   )\n>Archer  ( HP++  | DMG+++  )\n>Mage    ( HP+   | DMG++++ )\n")
    hClass = hClass.lower()

hHP = 0

if hClass == "warrior":
    hHP = 60
    hMaxHP = 60
    hHPpc = int(hHP / hMaxHP * 100)
    hWeapon = "Wooden Sword"
    hMinDMG = 7
    hMaxDMG = 9
    hAttack1 = " swings his sword at "
    hAttack2 = ""

elif hClass == "archer":
    hHP = 50
    hMaxHP = 50
    hHPpc = int(hHP / hMaxHP * 100)
    hWeapon = "Fragile Bow"
    hMinDMG = 9
    hMaxDMG = 11
    hAttack1 = " shoots an arrow at "
    hAttack2 = ""

elif hClass == "mage":
    hHP = 40
    hMaxHP = 40
    hHPpc = int(hHP / hMaxHP * 100)
    hWeapon = "Weakened Staff"
    hMinDMG = 11
    hMaxDMG = 13
    hAttack1 = " hits "
    hAttack2 = " with a fire ball"

elif hClass == "godmodegg":
    hHP = 1000
    hMaxHP = 1000
    hHPpc = int(hHP / hMaxHP * 100)
    hWeapon = "God's Touch"
    hMinDMG = 100
    hMaxDMG = 200
    hAttack1 = " destroys "
    hAttack2 = " with his touch"

hGold = 100

if hClass == "human":
    Area = "Gondor"
elif hClass == "elf":
    Area = "Rivendel"
elif hClass == "dwarf":
    Area = "Edoras"
elif hClass == "orc":
    Area = "Mordor"
elif hClass == "uruk-hai":
    Area = "Isengard"
elif hClass == "goblin":
    Area = "Moria"
elif hClass == "tee":
    Area = "iF City"

  #Moria Enemies:
#Goblin Stats:
Goblin_MinHP = 38
Goblin_MaxHP = 45
Goblin_MinDMG = 6
Goblin_MaxDMG = 9
Goblin_MinGold = 37
Goblin_MaxGold = 54

#Boar Stats:
Boar_MinHP = 32
Boar_MaxHP = 39
Boar_MinDMG = 8
Boar_MaxDMG = 12
Boar_MinGold = 20
Boar_MaxGold = 34


#Spider Stats:
Spider_MinHP = 40
Spider_MaxHP = 50
Spider_MinDMG = 10
Spider_MaxDMG = 13
Spider_MinGold = 44
Spider_MaxGold = 57


def battle():
    global hHP
    Enemy = randint(1,100)
    if Area == "Jungle":
        if Enemy < 51:
            eName = "Goblin"
            eHP = randint(Goblin_MinHP,Goblin_MaxHP)
            eMinDMG = Goblin_MinDMG
            eMaxDMG = Goblin_MaxDMG
            mGold = Goblin_MinGold
            MGold = Goblin_MaxGold
        elif Enemy > 50:
            eName = "Wild Boar"
            eHP = randint(Boar_MinHP,Boar_MaxHP)
            eMinDMG = Boar_MinDMG
            eMaxDMG = Boar_MaxDMG
            mGold = Boar_MinGold
            MGold = Boar_MaxGold

    btRound = 0

    print("\n\n\n    «===[Area 1: Jungle]===»\n")
    input("Press ENTER to start fight.")
    while eHP > 0 and hHP > 0:

        #Begin next round
        btRound = btRound + 1
        print("\n\n\n    «===[Round "+str(btRound)+"]===»")
        print("\n\n>"+hName+" has "+str(hHP)+" HP")
        print(">"+str(eName)+" has "+str(eHP)+" HP")

        #Deal damage
        hDMG = randint(hMinDMG,hMaxDMG)
        eHP = int(eHP - hDMG)
        eDMG = randint(eMinDMG,eMaxDMG)
        hHP = int(hHP - eDMG)

        #Set negative HPs to 0
        if eHP < 0:
            eHP = 0
        if hHP < 0:
            hHP = 0

        #Describe what happened
        print("\n\n>"+hName+hAttack1+str(eName)+hAttack2+" dealing "+str(hDMG)+" damage.")
        print(">"+eName+" strikes "+hName+" dealing "+str(eDMG)+" damage")

        #End round
        print("\n\n>"+hName+" has "+str(hHP)+" HP")
        print(">"+str(eName)+" has "+str(eHP)+" HP")
        time.sleep(0.5)

    #Check who died (you,enemy or both)
    if hHP < 1 and eHP < 1:
        print("\n\nYou both died, NUBS!")
        
    elif eHP < 1:
        print("\n\nYOU WIN! GG\n")
        time.sleep(0.5)
        global hGold
        EarnChance = randint(1,100)
        if EarnChance < 35:
            print("You didn't find anything.")
        else:
            EarnedGold = randint(mGold,MGold)
            hGold = hGold + EarnedGold
            print("You found "+str(EarnedGold)+" gold, Total Gold: "+str(hGold))
        time.sleep(0.5)
    
    elif hHP < 1:
        print("\n\nYOU LOSE! :O")
    input("Press ENTER to continue.")

#Show Stats
def ShowStats():
    print("\n\nName: "+hName)
    print("Kind: "+hRace)
    print("Class: "+hClass)
    print("HP: "+str(hHP)+"/"+str(hMaxHP)+" | "+str(hHP / hMaxHP * 100)+"%")
    print("Weapon: "+hWeapon+" | "+str(hMinDMG)+"-"+str(hMaxDMG)+" damage")

def Rest():
    global hHP
    if hHP < hMaxHP:
        hpRecovery = randint(20,30)
        hHP = int(hHP + hpRecovery)
        if hHP > hMaxHP:
            hHP = hMaxHP
        print("You rested for a while and recovered "+str(hpRecovery)+" HP.")
    else:
        print("You're HP is already fully recovered!")

def NextAction():
    NextAct = input('\n')
    NextAct = NextAct.lower()
    if NextAct not in Actions:
        if NextAct != "":
            print('\nInvalid action!\nIf you wish to see all possible actions, answer "Actions".')
            NextAction()
        else:
            NextAction()
    else:
        if NextAct == "fight":
            if hHP / hMaxHP * 100 > 20:
                battle()
                NextAction()
            else:
                print("You're too weak")
                NextAction()
        elif NextAct == "actions":
            print("\nPossible Actions:\n"+str(Actions))
            NextAction()
        elif NextAct == "stats":
            ShowStats()
            NextAction()
        elif NextAct == "rest":
            Rest()
            NextAction()
        elif NextAct == "leave":
            print("Bye!")
            time.sleep(1)
        elif NextAct == "contact":
            print("\nHow to contact me:\nDiscord: WestEUspy#6020\nTwitter: WesternEuropeSpy_PT")
            NextAction()

Area = "Jungle"

print("\nWatch out!")
battle()

time.sleep(0.4)
print("\nLoading quick message from developer...\n")
time.sleep(0.4)
print(".")
time.sleep(0.4)
print("..")
time.sleep(0.4)
print("...")
time.sleep(0.4)

print("\nThis game is still in early development.\nIf you find any bugs please tell me.")
input("If you don't know how to,"+' say "contact"')
print("\nThank you for playing ^_^")

NextAction()