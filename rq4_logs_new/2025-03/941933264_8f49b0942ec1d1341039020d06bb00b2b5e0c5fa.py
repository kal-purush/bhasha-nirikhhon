import datetime
import time

# Get current date
current_date = datetime.date.today().strftime('%Y-%m-%d')
current_date_lst = current_date.split('-')

# Predefined birthday log
bday_log = [
   ('Ayushi', ('1999', '10', '19')),
   ('Yash', ('1999', '04', '21')),
]

# Ask the user if they want to add a birthday
add = input('To add birthday, type y: ').strip().lower()

if add[:1] == 'y':
    new = input('Add birthday in format yyyy-mm-dd: ').strip()
    name = input('Whose birthday is it? ').strip()
    
    try:
        # Validate date format
        date = tuple(new.split('-'))
        if len(date) == 3 and all(part.isdigit() for part in date):
            bday_log.append((name, date))
        else:
            print("Invalid date format. Please enter in YYYY-MM-DD format.")
    except Exception as e:
        print(f"Error: {e}")

# Open the log file for appending
log_file = "birthday_log.txt"

print("\nBirthday logger is running... Press CTRL+C to stop.\n")

# Run indefinitely (update log every 3 seconds)
try:
    while True:
        current_time = datetime.datetime.now().strftime('%H:%M:%S')
        with open(log_file, "a") as log:
            for birthday in bday_log:
                if current_date_lst[1] == birthday[1][1] and current_date_lst[2] == birthday[1][2]:
                    age = int(current_date_lst[0]) - int(birthday[1][0])
                    
                    # Determine ordinal suffix (st, nd, rd, th)
                    if 11 <= age % 100 <= 13:
                        ordinal_suffix = "th"
                    else:
                        ordinal_suffix = {1: "st", 2: "nd", 3: "rd"}.get(age % 10, "th")

                    birthday_message = f"{current_time} - Happy Birthday {birthday[0]}! 🎉 It's their {age}{ordinal_suffix} Birthday!"
                    print(birthday_message)

                    # Log to file
                    log.write(f"{current_date} {current_time}: {birthday_message}\n")
        
        # Wait for 3 seconds before next log entry
        time.sleep(3)

except KeyboardInterrupt:
    print("\nBirthday logger stopped. 🎂")