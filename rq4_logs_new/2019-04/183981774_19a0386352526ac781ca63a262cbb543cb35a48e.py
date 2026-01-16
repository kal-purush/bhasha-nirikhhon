import sys


class Member:
    def __init__(self,member_name,phone_no,jersey_no):
        self.member_name=member_name
        self.phone_no=phone_no
        self.jersey_no=jersey_no

menu_choice = 0
team_roster_dict = dict()

while menu_choice != 9:
    print("\n\nWelcome to the Team Manager")
    print("===========Main Menu===========")
    print("1. Display the Team Roster")
    print("2. Add Member")
    print("3. Remove Member")
    print("4. Edit Member")
    print("5. Save Data")
    print("6. Load Data")
    print("9. Exit Program")
    
    menu_choice = int(input("Selection> "))
    
    if menu_choice == 1:
        current = 0
        for team_member in team_roster_dict:
            print("Name: ",team_roster_dict[team_member].member_name)
            print("Phone: ",team_roster_dict[team_member].phone_no)
            print("Jersey Number: ",team_roster_dict[team_member].jersey_no)
        if len(team_roster_dict) is 0:
            print("Team Roster is empty")
    elif menu_choice == 2:
        member_name = input("Enter new member's name: ")
        phone=input("Contact phone number: ")
        jersey=input("Jersey number: ")
        member_info=Member(member_name,phone,jersey)
        team_roster_dict[member_name]=member_info
    elif menu_choice == 3:
        member_to_del = input("Enter member name to be removed: ")
        if member_to_del in team_roster_dict:
            del team_roster_dict[member_to_del]
        else:
            print(member_to_del, " was not found in the ream roster.")
    elif menu_choice == 4:
        member_to_edit = input("Enter the name of the member you want to edit: ")
        if member_to_edit in team_roster_dict:
            team_member = team_roster_dict[member_to_edit]
            team_member.member_name = input("Enter the new name of the member: ")
            team_member.phone_no=input("New contact phone number: ")
            team_member.jersey_no=input("New jersey number: ")
            del team_roster_dict[member_to_edit]
            team_roster_dict[team_member.member_name]=team_member
        else:
            print(member_to_edit, " was not found in the team roster.")
    elif menu_choice == 5:
        fileName=input("Filename to save: ")
        ouput=open(fileName,"w")
        for member_name in team_roster_dict:
            team_member=team_roster_dict[member_name]
            ouput.write(team_member.member_name+","+team_member.phone_no+","+team_member.jersey_no+"\n")
        ouput.close()
        print("Saving data...")
        print("Data saved.")
    elif menu_choice == 6:
        print("Please note that file's should be formatted as follows:\n")
        print("full name, position, ")
        fileName=input("Filename to load: ")
        inputFile=open(fileName,"r")
        for line in inputFile:
            line=line.strip()
            data=line.split(",")
            member_info=Member(data[0],data[1],data[2])
            if member_info.member_name not in team_roster_dict:
                 team_roster_dict[member_info.member_name]=member_info
        print("Loading data...")
        print("Data Loaded Successfully")
    elif menu_choice == 9:
        print("Exiting Program")
        sys.exit()
    else:
        print("Invalid menu choice. Try again.")