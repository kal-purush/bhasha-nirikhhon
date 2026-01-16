# Brynne DuBois
# Professor Bui
# Hackers in the Bazaar
# Project 04

# Team Roster Management Tool
# Repurposed data structures project (C++ -> Python)

class Athlete:
    def __init__(self, first_last, position, number, height, weight, hometown, home_state, school):
        self.first_last = first_last
        self.position = position
        self.number = number
        self.height = height
        self.weight = weight
        self.hometown = hometown
        self.home_state = home_state
        self.school = school

menu_choice = 0
roster_dict = dict()

while menu_choice != 7:
    print("\n\nWelcome to the Team Roster Management System!")
    print("********************Main Menu********************")
    print("1. Display the Team Roster")
    print("2. Add Athlete")
    print("3. Remove Athlete")
    print("4. Edit Athlete Info")
    print("5. Save Data")
    print("6. Load Data")
    print("7. Exit Program")

    menu_choice = int(input("Selection > "))

    if menu_choice == 1:
        current = 0
        for team_member in roster_dict:
            print("Full Name: ", roster_dict[team_member].first_last)
            print("Position: ", roster_dict[team_member].position)
            print("Number: ", roster_dict[team_member].number)
            print("Height: ", roster_dict[team_member].height)
            print("Weight: ", roster_dict[team_member].weight)
            print("Hometown: ", roster_dict[team_member].hometown)
            print("State: ", roster_dict[team_member].home_state)
            print("School: ", roster_dict[team_member].school)
        if len(roster_dict) is 0:
            print("The team roster is currently empty.")
    elif menu_choice == 2:
        print("Please enter the following information for the new athlete:\n")
        first_last = input("Full Name: ")
        position = input("Position Played: ")
        number = input("Jersey Number: ")
        height = input("Height: ")
        weight = input("Weight: ")
        hometown = input("Hometown: ")
        home_state = input("State: ")
        school = input("High School: ")
        athlete_info = Athlete(first_last, position, number, height, weight, hometown, home_state, school)
        roster_dict[first_last] = athlete_info
    elif menu_choice == 3:
        athlete_to_delete = input("Enter the name of the athlete to be removed: ")
        if athlete_to_delete in roster_dict:
            del roster_dict[athlete_to_delete]
        else:
            print(athlete_to_delete, " was not found in the team roster.")
    elif menu_choice == 4:
        athlete_to_edit = input("Enter the name of the athlete you want to edit: ")
        if athlete_to_edit in roster_dict:
            team_member = roster_dict[athlete_to_edit]
            print("Make changes to whichever field needs to be edited.")
            team_member.first_last = input("Enter the new name of the athlete: ")
            team_member.position = input("New position: ")
            team_member.height = input("New height: ")
            team_member.weight = input("New weight: ")
            team_member.hometown = input("New hometown: ")
            team_member.home_state = input("New state: ")
            team_member.school = input("New school: ")
            del roster_dict[athlete_to_edit]
            roster_dict[team_member.first_last] = team_member
        else:
            print(athlete_to_edit, " was not found in the team roster.")
    elif menu_choice == 5:
        file_name = input("What file do you want to save? ")
        output = open(file_name, "w")
        for first_last in roster_dict:
            team_member = roster_dict[first_last]
            output.write(team_member.first_last+","+team_member.position+","+team_member.number+","+team_member.height+","+team_member.weight+","+team_member.hometown+","+team_member.home_state+","+team_member.school+"\n")
        output.close()
        print("Saving data...")
        print("Data saved!")
    elif menu_choice == 6:
        file_name = input("What file do you want to load? ")
        input_file = open(file_name, "r")
        for line in input_file:
            line = line.strip()
            data = line.split(",")
            member_info = Athlete(data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7])
            if member_info.first_last not in roster_dict:
                roster_dict[member_info.first_last]=member_info
        print("Loading data...")
        print("Data loaded successfully!")
    elif menu_choice == 7:
        print("Exiting program.")
    else:
        print("Invalid menu choice. Please enter a valid option.")