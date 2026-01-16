'''
The total number of months included in the dataset

The net total amount of "Profit/Losses" over the entire period

The average of the changes in "Profit/Losses" over the entire period

The greatest increase in profits (date and amount) over the entire period

The greatest decrease in losses (date and amount) over the entire period
'''
#import required modules
import os
import csv
import sys

# set the columns of excel  and new column as emoty list.
month=[]
profit_losses=[]
month_chnage=[]

#set the path to read the excel file.
budget_csv = os.path.join("Resources", "budget_data.csv")


#open the CSV file 
with open(budget_csv, newline="") as csvfile:
    csvreader = csv.reader(csvfile, delimiter=",")
    #header .column name 
    csv_header = next(csvreader)
    #print(f"CSV Header: {csv_header}")

    #append to empty  lists by reading each row 
    for row in csvreader:
        month.append(row[0])
        profit_losses.append(int(row[1]))
    #print(month[0:5])
    #print(profit_losses[0:5])

    #calculate total months 
    total_month=len(month)
    #print(total_month)

    #calculate net_profit/loss
    net_profit_loss=0
    for i in profit_losses:
        net_profit_loss+=i
    #print(net_profit_loss)

    #Calculate chnage in profit from last perod.,for first period I put 0
    month_chnage.append(0)
    for i in range(1,len(profit_losses)):
        month_chnage.append(profit_losses[i]-profit_losses[i-1])
    #print(month_chnage[0:10])


    #calculate Maximum increase and decrease of chnage 
    max_increase=max(month_chnage)
    max_decrease=min(month_chnage)
    #print(max_increase,month[month_chnage.index(max_increase)])
    #print(max_decrease,month[month_chnage.index(max_decrease)])
    

    '''
    work on the output format
    Financial Analysis
    ----------------------------
    Total Months: 86
    Total: $38382578
    Average  Change: $-2315.12
    Greatest Increase in Profits: Feb-2012 ($1926159)
    Greatest Decrease in Profits: Sep-2013 ($-2196167)
    '''
    print('Financial Analysis')
    print ('----------------------------')
    print(f'Total Months:{total_month}') 
    print(f'Total:  ${net_profit_loss}')
    print(f'Greatest Increase in Profits:{month[month_chnage.index(max_increase)]}(${max_increase})')
    print(f'Greatest Dncrease in Profits:{month[month_chnage.index(max_decrease)]}(${max_decrease})')


    #export result to text file 
    sys.stdout = open('result.txt', 'w')
    print('Financial Analysis')
    print ('----------------------------')
    print(f'Total Months:{total_month}') 
    print(f'Total:  ${net_profit_loss}')
    print(f'Greatest Increase in Profits:{month[month_chnage.index(max_increase)]}(${max_increase})')
    print(f'Greatest Dncrease in Profits:{month[month_chnage.index(max_decrease)]}(${max_decrease})')

    sys.stdout.close()




     
    
    
    
        