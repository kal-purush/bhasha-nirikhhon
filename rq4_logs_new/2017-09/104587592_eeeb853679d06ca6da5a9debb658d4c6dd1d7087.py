import random
import math
#player stats
plStr = 4
plBdy = 4
plRef = 4
plMeA = 4
plPer = 4
plLvl = 1

#placeholders
moName = "Hynithid"
moStr = 10
moBdy = 7
moRef = 1
moMeA = 5
moPer = 2

fighting = True
playerDead = False



#derived stats
def playerStats(plStr, plBdy, plRef, plMeA, plPer, plLvl):
	plHP = plBdy * 10 * plLvl
	plDmg = int(plStr / 2)
	plDef = plRef - 1
	return (plHP, plDmg, plDef, plStr, plBdy, plRef, plMeA, plPer, plLvl )

def monsterStats(moName, moStr, moBdy, moRef, moMeA, moPer, plLvl):
	#Hynithid Elemental/Medium/Defensive', 'Strength': 10, 'Body': 7, 'Reflexes': 1, 'MentalAptitude': 5, 'Perception': 2}
	moHP = moBdy * 10 * plLvl
	moDmg = int(moStr / 2)
	moDef = moRef - 1
	return (moName, moHP, moDmg, moDef, moStr, moBdy, moRef, moMeA, moPer, plLvl)

monsterDead = False
goodguy = playerStats(plStr, plBdy, plRef, plMeA, plPer, plLvl)
badguy = monsterStats(moName, moStr, moBdy, moRef, moMeA, moPer, plLvl)
badguyhp = badguy[2]
goodguyhp = goodguy[0]	


while monsterDead == False and playerDead == False:

	print ('You are at ', goodguyhp, 'hit points')
	if badguyhp > (.75*badguy[2]):
		print('The', badguy[0], 'does not look all that happy to see you.') 
	elif badguyhp > (.50*badguy[2]):
		print('The', badguy[0], 'is bloodied, but still staring your down.') 
	elif badguyhp > (.25*badguy[2]):
		print('The', badguy[0], 'is trying to take you down alongside...')
	else:
		print('The', badguy[0], 'is barely standing... let alone fighting')

	action = input('Do you (A)ttack or (R)un?')

	#print (goodguy[2])

	if action.lower()[0] == 'a':
		atkresult = goodguy[2] + random.randint(0,(goodguy[2])) - int(badguy[3]/2)
		
		if atkresult < 1:
			atkresult = 1
		else:
			print (atkresult, 'damage done!')
		badguyhp = badguyhp - atkresult
		
		if badguyhp <= 0:
			monsterDead = True
			print('You bested the encounter!')

		if monsterDead != True:
			monattack = badguy[2] + random.randint(0,(badguy[2])) - int(goodguy[2]/2)
		
			if monattack < 1:
				monattack = 1
			else:
				print ('It strikes for', monattack, 'damage!')
				goodguyhp = goodguyhp - monattack
	else:
		print ('Time to run like a cowardly cur...')
		print ('It strikes at you as you turn to flee:')
		if monsterDead != True:
			monattack = badguy[2] + random.randint(0,(badguy[2])) - int(goodguy[2]/2)
		
			if monattack < 1:
				monattack = 1
			else:
				print ('It strikes for', monattack, 'damage!')
				goodguyhp = goodguyhp - monattack

		runchance = random.randint(0,goodguy[5])
		catchchance = random.randint(0,(badguy[5] + badguy[7]))
		if runchance < catchchance:
			print ('You are unable to escape!')
		if runchance >= catchchance:
			print ('You are able to escape!')
			monsterDead = True
		print('runchance', runchance)
		print('catchchance', catchchance)

	if goodguyhp <= 0:
		playerDead=True
	else:
		playerDead=False




		


	#figure out what badguy
