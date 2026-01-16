import os
import json
import datetime

data_file = 'data.json'
data = []

# Ensure filepath exists, otherwise create one
if not os.path.exists(data_file):
    with open(data_file, 'w') as file:
        json.dump([], file)
    print("File data.json not found. New file created.")
else:
    with open(data_file, 'r') as file:
        data = json.load(file)

def get_hours():
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")

    # Prevent logging hours more than once per day
    for entry in data:
        if entry["date"] == current_date:
            print(f"You've already logged hours for {current_date}.")
            return None  

    while True:
        try:
            hours = float(input("How many hours did you study today? \n"))
            if hours < 0:
                print("Hours cannot be negative. Please enter a valid number.")
                continue
            break  
        except ValueError:
            print("Please enter a valid number for hours.")
    
    # Return dictionary for today's entry
    new_entry = {
        "date": current_date,
        "hours_studied": hours,
        "time_logged": datetime.datetime.now().strftime("%H:%M:%S")
    }
    return new_entry
    print(f"{data[-1]['hours_studied']} hours logged.")

def export_data():
    try:
        with open(data_file, 'w') as file:
            json.dump(data, file, indent=4)
        print("Data exported successfully.")
    except (IOError, json.JSONDecodeError) as e:
        print(f"There was an issue exporting your data: {e}")

def check_hours():
    while True:
        date_to_check = input("Please enter the date you would like to check (yyyy-mm-dd): \n")
        try:
            valid_date = datetime.datetime.strptime(date_to_check, "%Y-%m-%d")
            break
        except ValueError:
            print("Invalid date format or value. Please try again.")
    for entry in data:
        if entry["date"] == date_to_check:
            print(f"Hours logged for {date_to_check}: {entry['hours_studied']}")
            return None
    print("No hours have been logged for this date.")

def main():
    while True:
        option = input("Welcome to Study Tracker. You are in the main menu. What would you like to do? \n 1. Log hours studied \n 2. Export session data \n 3. Check hours logged on a day \n 4. Exit \n")
        if option == "1":
            new_entry = get_hours()  # Get the new entry
            if new_entry:  # Only append if the entry is valid (not None)
                data.append(new_entry)
        elif option == "2":
            export_data()   # Save the data
        elif option == "3":
            check_hours()   # Check hours studied on a given date
        elif option == "4":
            break   # Exit the program
        else:
            print("Please enter a valid option.")


main()

# Planned features:
# - predict amount of hours i will study in the whole month
# - recognize streaks
# - visualize data