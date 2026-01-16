# Import required Python libraries
import csv
import random
from faker import Faker

# Initialize Faker (used to generate fake data)
fake = Faker()

# Get user input for how many rows to generate and the filename for the CSV file
num_rows = int(input("Enter the number of rows the csv file should have: "))
csv_file = input("Enter the name of the csv file (e.g., live_data.csv): ")

# Open the CSV file in write mode ('w') and automatically close it after writing
with open(csv_file, mode="w", newline="") as file:
    writer = csv.writer(file)  # Create a CSV writer object to write data to the file

    # Writing the header row (column names)
    header = [
        "First Name",
        "Last Name",
        "Gender",
        "DateOfBirth",
        "Email",
        "Phone Number",
        "Address",
        "City",
        "State",
        "Postal Code",
        "Country",
        "LoyaltyProgramID",
    ]
    writer.writerow(header)  # Write the header row into the CSV file

    # Loop to generate fake data for each row, based on the number of rows entered by the user
    for _ in range(num_rows):
        # Creating a list of fake data for one row
        row = [
            fake.first_name(),
            fake.last_name(),
            random.choice(["M", "F", "Others", "Not Specified"]),
            fake.date(),
            fake.email(),
            fake.phone_number(),
            fake.address().replace(",", " ").replace("\n", " "),
            fake.city(),
            fake.state(),
            fake.postcode(),
            fake.country(),
            random.randint(1, 5),
        ]

        writer.writerow(row)

print("The file has been loaded successfully")