import random

monster_manual = []
enemy_cat = ["Humanoid", "Beast", "Monster", "Elemental"]
enemy_size = ["Tiny", "Small", "Medium", "Large", "Giant"]
enemy_style = ["Defensive", "Offensive", "Balanced", "Guardian", "Assailant"]
enemy_prefix = ['Rin', 'Ali', 'Ori', 'Grin', 'Pli', 'Hori', 'Zyl', 'Qyl', 'Ign', 'Jor', 'Myl', 'Plo', 'Hyn', 'Sty', 'HAk']
enemy_suffix = ['blin', 'k', 'c', 'nth', 'inoid', 'ster', 'sus', 'ity', 'mare', 'ithid', 'alli', 'thian', 'ron', 'less']

def thing():
	
	Ecat = Esize = Estyle = ""
	
	#determine category
	
	#print(len(enemy_cat))
	#print(len(enemy_size))
	#print(len(enemy_style))
	Ecat = random.randint(0,(len(enemy_cat))-1)
	Esize = random.randint(0,(len(enemy_size))-1)
	Estyle = random.randint(0,(len(enemy_cat))-1)
	Eprefix = random.randint(0,(len(enemy_prefix))-1)
	Esuffix = random.randint(0,(len(enemy_suffix))-1)

	return (Ecat, Esize, Estyle, Eprefix, Esuffix)

def MakeAMonster():

	junk = 0
	MMiteration = 0
	duplicates = 0
	MMcomplete = False

	while MMcomplete == False:

		dothething = (thing())
		resultName = str((enemy_prefix[thing()[3]])) + str((enemy_suffix[thing()[4]]))
		resultEnemy = [enemy_cat[thing()[0]], enemy_size[thing()[1]], enemy_style[thing()[2]]]
		#print (resultName)
		#print (resultEnemy)

		Stat_Str = 5
		Stat_Body = 5
		Stat_Ref = 5
		Stat_MA = 5
		Stat_Perc = 5

		if (resultEnemy[1]) == 'Tiny': #Tiny
		# effect Tiny 	Str/Bod-3	ref +3	perc +2
			Stat_Str -= 3
			Stat_Body-= 3
			Stat_Ref += 3
			Stat_Perc+= 2
		elif (resultEnemy[1]) == 'Small': #Small	
		# effect Small 	Str/Bod-2	ref +2	perc +1
			Stat_Str -= 2
			Stat_Body-= 2
			Stat_Ref += 2
			Stat_Perc+= 1
		elif (resultEnemy[1]) == 'Medium': #Medium
			Stat_Str -= 0
			Stat_Body-= 0
			Stat_Ref += 0
			Stat_Perc+= 0
		elif (resultEnemy[1]) == 'Large': #Large
		# effect Large	str/bod +2	ref -2	perc -1
			Stat_Str += 2
			Stat_Body+= 2
			Stat_Ref -= 2
			Stat_Perc-= 1
		elif (resultEnemy[1]) == 'Giant': #Giant
		# effect Giant	str/bod +3	Ref -3	perc -2
			Stat_Str += 3
			Stat_Body+= 3
			Stat_Ref -= 3
			Stat_Perc-= 2
		else:
			Stat_Str += 0
			Stat_Body+= 0
			Stat_Ref -= 0
			Stat_Perc-= 0

		if (resultEnemy[2]) == 'Guardian': #Guardian
			Stat_Body+= 2
			Stat_Ref += 2
			Stat_Str -= 3
			Stat_Perc+= 2
		elif (resultEnemy[2]) == 'Defensive': #Defensive
			Stat_Body+= 1
			Stat_Ref += 1
			Stat_Str -= 2
			Stat_Perc+= 1
		elif (resultEnemy[2]) == 'Balanced': #Balanced
			Stat_Body+= 0
			Stat_Ref += 0
			Stat_Str -= 0
			Stat_Perc+= 0
		elif (resultEnemy[2]) == 'Offensive': #Offensive
			Stat_Body-= 1
			Stat_Ref -= 1
			Stat_Str += 2
			Stat_Perc-= 1
		elif (resultEnemy[2]) == 'Assailant': #Assailant
			Stat_Body-= 2
			Stat_Ref -= 2
			Stat_Str += 3
			Stat_Perc-= 2
		else:
			Stat_Str += 0
			Stat_Body+= 0
			Stat_Ref -= 0
			Stat_Perc-= 0


		if Stat_Str < 1:
			Stat_Str = 1
		else:
			junk = 0
		
		if Stat_Body < 1:
			Stat_Body = 1
		else:
			junk = 0
			
		if Stat_Ref < 1:
			Stat_Ref = 1
		else:
			junk = 0

		if Stat_MA < 1:
			Stat_MA = 1
		else:
			junk = 0

		if Stat_Perc < 1:
			Stat_Perc = 1
		else:
			junk = 0

			
		monster_manual.append ({'Name': resultName, 'Category': enemy_cat[thing()[0]], 'Size': enemy_size[thing()[1]],
				'Style': enemy_style[thing()[2]], 'Strength': Stat_Str, 'Body': Stat_Body,'Reflexes': Stat_Ref,'MentalAptitude':Stat_MA,
				'Perception': Stat_Perc,})
		
		for num in range (0, MMiteration):
			if monster_manual[num]['Name'] == resultName:
				del monster_manual[num]
				duplicates +=1
				MMiteration -=1


		MMiteration += 1
		if duplicates == 20:
			MMcomplete = True

	for i in range (0, len(monster_manual)-1):
		print (monster_manual[i])
		print ()

	return(monster_manual)

ItsABook = MakeAMonster()
# print (len(MakeAMonster()))