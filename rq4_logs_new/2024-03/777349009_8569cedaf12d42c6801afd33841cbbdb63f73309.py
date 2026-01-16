import math
# project to help people calculate amounts of interest earned or bond to pay
# user inputs whether they have an investment or a bond
print("investment - to calculate the amount of interest you'll earn on your investment")
print("bond       - to calculate the amount you'll have to pay on a home loan")
print("")

# user decides on investment or bond
user_choice = input("Enter either 'investment' or 'bond' from the menu above to proceed: ")

# user chooses investment - they are asked for amount deposited, interet rate, and years they will invest - they are then returned the profit
if user_choice.lower() == "investment":
    investment_return = 0
    investment_deposit = int(input("Enter amount being deposited: "))
    investment_interest = int(input("Enter interest rate: "))
    investment_years = int(input("Enter number of years you plan to invest: "))
    interest_type = input("Please enter whether you would like 'simple' or compound' interest: ")

    # user must choose between simple and compound interest
    if interest_type == "simple":
        investment_return += investment_deposit * (1 + (investment_interest/100 * investment_years))
        print(f"Your investment return will be: {investment_return - investment_deposit}")

    elif interest_type == "compound":
        investment_return += investment_deposit * math.pow((1 + investment_interest/100), investment_years)
        print(f"Your investment return will be: {investment_return - investment_deposit}")

    else:
        print("Error")

# user chooses bond - they are asked for house value, interest and number of years to pay - monthly payments are returned        
elif user_choice.lower() == "bond":
    monthly_repayments = 0
    house_value = int(input("Please enter the current value of the house: "))
    bond_interest = int(input("Please enter the interest rate: "))
    months_to_repay = int(input("Please enter the number of months you intent to pay back your bond in: "))
    monthly_interest = (bond_interest / 100) / 12
    monthly_repayments += ((monthly_interest * house_value)) / (1 - (1 + monthly_interest) ** (-months_to_repay))
    print(f"Your monthly repayments are: {math.ceil(monthly_repayments)}.")

else:
    print("You have not input either 'investment' or 'bond', please try again.")

