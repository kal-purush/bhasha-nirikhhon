import copy
class Player:
    def __init__(self):
        pass

    def play(self, dice_face, available_rerolls):
       dice = copy.deepcopy(dice_face)
       dice_dict = {} #the dictionary for dice
       ring_ding = False
       
       #puts the values into a dictionary
       for x in dice:
           if x != dice_dict.keys():
               dice_dict.setdefault(x, dice.count(x))
           else:
               dice_dict[x] = dice_dict[x] + 1

       #will be used to see what moves can be get from the dice_dict
       conditions = {
           '5-In-A-Row': False,
           'Straight': False,
           'Full house': False,
           'Four-of-a-kind': False,
           'Three-of-a-kind': False,
           'Shitty combo' : True #rework this to force a reroll of all dice
       }

       #production rules to find the approriate roll
       if (5 in dice_dict.values()) or (4 in dice_dict.values()) or (3 in dice_dict.values()):
           conditions['5-In-A-Row'] = True

       #this needs to be reowrked BLACK SHEEP BLACK SHEEP
       if 69 == 68:#(1 and 2 and 3 and 4 and 5) in dice_dict.keys() or (2 and 3 and 4 and 5 and 6) in dice_dict.keys() or len(dice_dict.keys()) == 4:
           conditions['Straight'] = True

       if (3 and 2) in dice_dict.values() or (3 and 1) in dice_dict.values() or (2 and 2) in dice_dict.values():
           conditions['Full house'] = True

       if (4 and 1) in dice_dict.values() or (3 and 1 and 1) in dice_dict.values() or (3 and 2) in dice_dict.values():
           conditions['Four-of-a-kind'] = True

       if (2 and 1 and 1 and 1) in dice_dict.values() or (3) in dice_dict.values():
           conditions['Three-of-a-kind'] = True

       #creates a list of possible moves the AI can do
       possible_moves = []#empty list to store possible moves
       for k,v in conditions.items():
           if v == True:
               possible_moves.append(k)

       move_probability={} #to store the name of the move and the probability for victory
       probability = 0 #initializes and refreshes the probability variable
       discrepancies = 0 #to find the total number of duplicates
       priority = 0 #lists probabilities by priority

       safelist_priority = {} #make a list of safe numbers with priorities 

       for k in possible_moves:
           safelist = []
           if k == '5-In-A-Row':
               priority = 5
               for z, v in dice_dict.items():
                   if v == 5:
                       ring_ding = True
                       break
                   elif v == 4:
                       safelist.append(z)
                       probability = 1 - (1/6)
                   elif v == 3:
                       safelist.append(z)
                       probability = 1 - 2*(1/6)

           if k == 'Straight':
               priority = 4
               #accesses dice_dict to reroll the numbers
               for z,v in dice_dict.items():
                   if v > 1:
                       discrepancies += v #to calculate dice to be rerolled
                   else:
                       straight_safenumbers.append(z) #saves the dice that don't need to be rerolled
               probability = 1 - discrepancies * (1/6) #finds the probability
               
           if k == 'Full house':
               priority = 3
               for z, v in dice_dict.items():
                   if v == 3:
                       safelist.append(z)
                   elif v == 2:
                       safelist.append(z)
                   else:
                       discrepancies += 1
               probability = 1 - discrepancies * (1/6)

           if k == 'Four-of-a-kind':
               print(dice_dict)
               priority = 2
               for z,v in dice_dict.items():
                   if v == 4:
                       safelist.append(z)
                       discrepancies = 0
                   elif v == 3:
                       safelist.append(z)
                       discrepancies = 1
                   elif v == 2:
                       safelist.append(z)
                       discrepancies = 2
               probability = 1 - discrepancies * (1/6)

           if k == 'Three-of-a-kind':
               priority = 1
               tak_safenumbers = []

               for z,v in dice_dict.items():
                   if 3 in dice_dict.values():
                       break
                   if v > 2 and v <= 3:
                       tak_safenumbers.append(z)
                   else:
                       discrepancies += 1
               probability = 1 - discrepancies * (1/6)

           if k == 'Shitty combo':
               priority = 6
               probability = 1 - discrepancies * (1/6)
               break
           probability = 100 * probability
           move_probability.setdefault(priority, probability)
           safelist_priority.setdefault(priority, safelist)

       #rule intepreter
       biggest_priority = 10
       biggest_probability = 0 #to find the highest probability
       priority_thresh = 0 #prevents the firing of low priority rules
       keepers = []
       for k,v in move_probability.items():
           #to find the best probablity
           if v > biggest_probability: #!= 100 because it will return an empty set
               #if probability = 100, set a threshold value
               if v == 100:
                   priority_thresh = k
               else:
                   biggest_probability = v
               if k < biggest_priority and k > priority_thresh:
                   biggest_priority = k

       print(biggest_priority)

       if ring_ding == True:
           safelist_priority.setdefault(0,0)
           biggest_priority = 0
       keepers = safelist_priority[biggest_priority] #find the numbers to keep

       if ring_ding == True:
           reroll = []
       else:
           reroll = [i for i, x in enumerate(dice_face) if x not in keepers] #returns indexes of dice to be rerolled

       return reroll