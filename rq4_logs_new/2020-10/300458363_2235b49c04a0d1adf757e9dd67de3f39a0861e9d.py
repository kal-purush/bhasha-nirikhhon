import os

import csv

csvpath = "C:\\Users\\alysh\\Desktop\\uncc-cha-data-pt-09-2020-u-c\\02-Homework\\03-Python\\python-challenge\\PyBank\Resources\\budget_data.csv"

total_months = 0
total_profit = 0
average_change = []
greatest_increase = ['', 0]
greatest_decrease = ['', 999999999999999]

with open(csvpath) as csvfile:
    csv_reader = csv.reader(csvfile, delimiter = ",")
    header = next(csv_reader)
    firstRow = next(csv_reader)
    total_months += 1
    total_profit += int(firstRow[1])
    prior_profit = int(firstRow[1])
    #print(firstRow)
    for row in csv_reader:
        #print(row)
        total_months += 1
        total_profit += int(row[1])
        #net change calculation
        profit_change = int(row[1]) - prior_profit
        prior_profit = int(row[1])
        average_change.append(profit_change)
        if profit_change > greatest_increase[1]:
            greatest_increase[0] = row[0]
            greatest_increase[1] = profit_change
        
        if profit_change < greatest_decrease[1]:
            greatest_decrease[0] = row[0]
            greatest_decrease[1] = profit_change
# calculate the average change
net_change = sum(average_change) / len(average_change)
#increase_profits = profit_change
        
print("Financial Analysis")
print("----------------------------")
print("Total Months: " + str(total_months))
print("Total: $" + str(total_profit))
print("Average Change: $" + str(net_change))
print("Greatest Increase in Profits: " + (greatest_increase[0]) +" $"+ str (greatest_increase[1]))
print("Greatest Decrease in Profits: " + (greatest_decrease[0]) + " $" + str (greatest_decrease[1]))