import os
import csv
# Set the path for the CSV file
PyBankcsv = os.path.join("resources","budget_data.csv")

# Create lists to store data
profit = []
monthly_changes = []
date = []

# Initialize variables
count = 0
total_profit = 0
total_change_profits = 0
initial_profit = 0

# Open the CSV file
with open(PyBankcsv, encoding='UTF-8') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=",")
    csv_header = next(csvreader)

    # Iterate through each row in the CSV file
    for row in csvreader:
        # Count the number of months
        count += 1

        # Append profit information and calculate total profit
        date.append(row[0])
        profit.append(int(row[1]))
        total_profit += int(row[1])

        # Calculate the monthly change in profits
        final_profit = int(row[1])
        monthly_change_profits = final_profit - initial_profit

        # Store monthly changes in a list
        monthly_changes.append(monthly_change_profits)

        total_change_profits += monthly_change_profits
        initial_profit = final_profit

    # Calculate the average change in profits
    average_change_profits = total_change_profits / (count - 1)

    # Find the greatest increase and decrease in profits
    greatest_increase_profits = max(monthly_changes)
    greatest_decrease_profits = min(monthly_changes)

    increase_date = date[monthly_changes.index(greatest_increase_profits)]
    decrease_date = date[monthly_changes.index(greatest_decrease_profits)]

    # Print the financial analysis
    print("----------------------------------------------------------")
    print("Financial Analysis")
    print("----------------------------------------------------------")
    print("Total Months: " + str(count))
    print("Total Profits: $" + str(total_profit))
    print("Average Change: $" + str(round(average_change_profits, 2)))
    print("Greatest Increase in Profits: " + increase_date + " ($" + str(greatest_increase_profits) + ")")
    print("Greatest Decrease in Profits: " + decrease_date + " ($" + str(greatest_decrease_profits) + ")")
    print("----------------------------------------------------------")

    # Write the results to a text file
    with open('financial_analysis.txt', 'w') as text:
        text.write("----------------------------------------------------------\n")
        text.write("  Financial Analysis\n")
        text.write("----------------------------------------------------------\n\n")
        text.write("    Total Months: " + str(count) + "\n")
        text.write("    Total Profits: $" + str(total_profit) + "\n")
        text.write("    Average Change: $" + str(round(average_change_profits, 2)) + "\n")
        text.write("    Greatest Increase in Profits: " + increase_date + " ($" + str(greatest_increase_profits) + ")\n")
        text.write("    Greatest Decrease in Profits: " + decrease_date + " ($" + str(greatest_decrease_profits) + ")\n")
        text.write("----------------------------------------------------------\n")