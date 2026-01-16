from random import randint

"""A program to roll and print a stat sheet for D&D 5e"""


class Dandd:
    """A Class that rolls dice, adds them and prints out the six stats needed to start a character"""

    def __init__(self, sides=6):
        self.sides = sides

    def roll_dice(self):
        """A single roll of the dice"""
        roll = randint(1, self.sides)
        return roll

    def roll_stat(self):
        """the sum of the three highest values out of four dice rolls"""
        roll_total = []
        stat_nums = []
        for g in range(4):
            h = self.roll_dice()
            roll_total.append(h)
            roll_total.sort()
        stat_nums = roll_total[-3:]
        return sum(stat_nums)

    def roll_abilities(self):
        """A function that returns six values that are the starting six ability stats before racial stat bonuses"""
        stat_totals = []
        for t in range(6):
            w = self.roll_stat()
            stat_totals.append(w)
            stat_totals.sort()
        return stat_totals

    def stat_roll_select(self):
        """A function that rolls 3 lines of stats and lets the user select one"""
        stat_list = {}
        print('Time to roll those abilities!\nYou will get three rolls to decide from.')
        for x in range(1, 4):
            y = self.roll_abilities()
            print(f'Roll {x}: {y}')
            stat_list[x] = y
        selection = 'Which roll would you like to use or type "quit" to start over?'
        selection += "\nType '1' for the first roll\nType '2' for the second roll\nType '3' for the third roll\n: "
        u = input(selection)
        if u == '1':
            return stat_list[1]
        elif u == '2':
            return stat_list[2]
        elif u == '3':
            return stat_list[3]
        elif u == 'quit':
            return 'good luck'

    def race_select(self):
        """Choose a race from a list with associated stats. stat_library established to hold ability values"""
        stat_library = {'STR': 0, 'DEX': 0, 'CON': 0, 'INT': 0, 'WIS': 0, 'CHA': 0}
        race = 'Choose a race to play. The format to select your race is as follows:'
        race += '\n(# to input)   //   Race name   //   Bonuses towards your stats (e.g. +2 Dex)'
        race += '\n(1)   //   Dragonborn    //   +2 STR, +1 CHA'
        race += '\n(2)   //   Dwarf         //   +2 CON'
        race += '\n(3)   //   Elf           //   +2 DEX'
        race += '\n(4)   //   Gnome         //   +2 INT'
        race += '\n(5)   //   Half-Elf      //   +2 CHA, +1 to two other ability scores of your choice'
        race += '\n(6)   //   Halfling      //   +2 DEX'
        race += '\n(7)   //   Half-Orc      //   +2 STR, +1 CON'
        race += '\n(8)   //   Human         //   +1 to all ability scores'
        race += '\n(9)   //   Tiefling      //   +2 CHA, +1 INT'
        race += '\nSelection: '
        number_for_race = input(race)
        if number_for_race == '1':
            char_race = 'Dragonborn'
            stat_library['STR'] += 2
            stat_library['CHA'] += 1
        elif number_for_race == '2':
            char_race = 'Dwarf'
            stat_library['CON'] += 2
        elif number_for_race == '3':
            char_race = 'Elf'
            stat_library['DEX'] += 2
        elif number_for_race == '4':
            char_race = 'Gnome'
            stat_library['INT'] += 2
        elif number_for_race == '5':
            char_race = 'Half-Elf'
            stat_library['CHA'] += 2
            distribute_1 = 'Where would you like to distribute your first +1?'
            distribute_1 += '\n1 = STR \n2 = DEX \n3 = CON \n4 = INT \n5 = WIS \n6 = CHA'
            distribute_1 += '\nSelection: '
            answer_1 = input(distribute_1)
            if answer_1 == '1':
                stat_library['STR'] += 1
            elif answer_1 == '2':
                stat_library['DEX'] += 1
            elif answer_1 == '3':
                stat_library['CON'] += 1
            elif answer_1 == '4':
                stat_library['INT'] += 1
            elif answer_1 == '5':
                stat_library['WIS'] += 1
            elif answer_1 == '6':
                stat_library['CHA'] += 1
            else:
                print("Looks like you could use some intelligence... +1!")
                stat_library['INT'] += 1
            distribute_2 = 'Where would you like to distribute your second +1?'
            distribute_2 += '\n1 = STR \n2 = DEX \n3 = CON \n4 = INT \n5 = WIS \n6 = CHA'
            distribute_2 += '\nSelection: '
            answer_2 = input(distribute_2)
            if answer_2 == '1':
                stat_library['STR'] += 1
            elif answer_2 == '2':
                stat_library['DEX'] += 1
            elif answer_2 == '3':
                stat_library['CON'] += 1
            elif answer_2 == '4':
                stat_library['INT'] += 1
            elif answer_2 == '5':
                stat_library['WIS'] += 1
            elif answer_2 == '6':
                stat_library['CHA'] += 1
            else:
                print("Looks like you could use some intelligence... +1!")
                stat_library['INT'] += 1
        elif number_for_race == '6':
            char_race = 'Halfling'
            stat_library['DEX'] += 2
        elif number_for_race == '7':
            char_race = 'Half-Orc'
            stat_library['STR'] += 2
            stat_library['CON'] += 1
        elif number_for_race == '8':
            char_race = 'Human'
            stat_library['STR'] += 1
            stat_library['DEX'] += 1
            stat_library['CON'] += 1
            stat_library['INT'] += 1
            stat_library['WIS'] += 1
            stat_library['CHA'] += 1
        elif number_for_race == '9':
            char_race = 'Tiefling'
            stat_library['CHA'] += 2
            stat_library['INT'] += 1
        else:
            print('Default race is human.')
            char_race = 'Human'
            stat_library['STR'] += 1
            stat_library['DEX'] += 1
            stat_library['CON'] += 1
            stat_library['INT'] += 1
            stat_library['WIS'] += 1
            stat_library['CHA'] += 1

        return char_race, stat_library

    def character_select(self):
        """A prompt to give your character a name and catch phrase. Runs race_select"""
        char_race, stat_library = self.race_select()
        name = 'Give a name to your champion: '
        char_name = input(name)
        phrase = "What is your character's catch phrase: "
        char_phrase = input(phrase)
        char_info = f'{char_name.title()} the {char_race}\n\t"{char_phrase}"'
        return char_info, stat_library

    def stat_distribution(self):
        """Lets the user distribute the attributes rolled to the stat_library."""
        char_info, stat_library = self.character_select()
        roll_selected = self.stat_roll_select()
        review_1 = "Let's review! Your character's stats are as follows:" \
            f"\nSTR (Strength).......{stat_library['STR']}" \
            f"\nDEX (Dexterity)......{stat_library['DEX']}" \
            f"\nCON (Constitution)...{stat_library['CON']}" \
            f"\nINT (Intelligence)...{stat_library['INT']}" \
            f"\nWIS (Wisdom).........{stat_library['WIS']}" \
            f"\nCHA (Charisma).......{stat_library['CHA']}" \
            f"\nAnd here is the roll you selected:" \
            f"\n{roll_selected}"
        print(review_1)
        dull_out = "\nLet's distribute those stats!\nStarting with the highest roll value, select which " \
                   "attribute to place each value. Remember - only one value per attribute."
        print(dull_out)
        extra_list = []
        roll_selected.reverse()
        for x in roll_selected:
            active_status = True
            while active_status:
                det_list = list(stat_library)
                choose_att = f"\nWhich attribute would you like to place the value: {x}" \
                f"\n(# to type) // Attribute" \
                f"\n(1) // {det_list[0]}" \
                f"\n(2) // {det_list[1]}" \
                f"\n(3) // {det_list[2]}" \
                f"\n(4) // {det_list[3]}" \
                f"\n(5) // {det_list[4]}" \
                f"\n(6) // {det_list[5]}" \
                f"\nSelection: "
                choice = int(input(choose_att)) - 1
                if choice in extra_list:
                    print('\nSelect another attribute.')
                    continue
                else:
                    stat_library[det_list[choice]] += x
                    extra_list.append(choice)
                    active_status = False
        review_2 = f"Here's what your character's stats look like:"
        str1 = (stat_library['STR'] - 10) // 2
        dex1 = (stat_library['DEX'] - 10) // 2
        con1 = (stat_library['CON'] - 10) // 2
        int1 = (stat_library['INT'] - 10) // 2
        wis1 = (stat_library['WIS'] - 10) // 2
        cha1 = (stat_library['CHA'] - 10) // 2
        review_2 += f"\n{char_info}" \
        f"\nAbility...........Ability number.....Ability modifier" \
        f"\nSTR (Strength)..........  {stat_library['STR']}  ......    {str1:+g}" \
        f"\nDEX (Dexterity).........  {stat_library['DEX']}  ......    {dex1:+g}" \
        f"\nCON (Constitution)......  {stat_library['CON']}  ......    {con1:+g}" \
        f"\nINT (Intelligence)......  {stat_library['INT']}  ......    {int1:+g}" \
        f"\nWIS (Wisdom)............  {stat_library['WIS']}  ......    {wis1:+g}" \
        f"\nCHA (Charisma)..........  {stat_library['CHA']}  ......    {cha1:+g}"
        return review_2

start = Dandd()
print(start.stat_distribution())