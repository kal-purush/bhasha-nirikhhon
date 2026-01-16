#!/usr/bin/env python
# coding: utf-8

# #### COFFEE MACHINE

# In[ ]:





# In[10]:


MENU = {"espresso": {"ingredients": {"water": 50, "coffee": 18,}, "cost": 1.5,},
        "latte": {"ingredients": {"water": 200, "milk": 150, "coffee": 24,}, "cost": 2.5,},
        "cappuccino": {"ingredients": {"water": 250, "milk": 100, "coffee": 24,}, "cost": 3.0,}}

profit = 0
resources = {"water": 300, "milk": 200, "coffee": 100,}


# In[11]:


def check_ingredients_qty(order_ingredients):
    """Returns True when order can be made, False if ingredients are insufficient."""
    for item in order_ingredients:
        if order_ingredients[item] > resources[item]:
            print(f"Sorry there is not enough {item}.")
            return False
    return True


# In[12]:


def make_coffee(drink_name, order_ingredients):
    """Deduct the required ingredients from the resources."""
    for item in order_ingredients:
        resources[item] -= order_ingredients[item]
    print(f"Here is your {drink_name} ☕️. Enjoy!")


# In[13]:


def money_paid():
    """Returns the total calculated from coins inserted."""
    print("Please insert coins.")
    total = int(input("how many quarters?: ")) * 0.25
    total += int(input("how many dimes?: ")) * 0.1
    total += int(input("how many nickles?: ")) * 0.05
    total += int(input("how many pennies?: ")) * 0.01
    total = round(total, 2)
    return total


# In[14]:


def check_transaction(drink_payment, cost_of_drink):
    """Return True when the payment is accepted, or False if money is insufficient."""
    if drink_payment >= cost_of_drink:
        change = round(drink_payment - cost_of_drink, 2)
        print(f'Here is your change ${change}')
        global profit
        profit += cost_of_drink
        return True
    else:
        print(f'Here is your money ${drink_payment} refunded.')
        return False


# In[15]:


is_on = True

while is_on:
    choice = input("Chooose the drink you want - espresso/latte/cappuccino: ")
    if choice == "off":
        is_on = False
    elif choice == "report":
        print(f"Water: {resources['water']}ml")
        print(f"Milk: {resources['milk']}ml")
        print(f"Coffee: {resources['coffee']}g")
        print(f"Money: ${profit}")
    else:
        drink = MENU[choice]
        if check_ingredients_qty(drink["ingredients"]):
            payment = money_paid()
            if check_transaction(payment, drink['cost']):
                make_coffee(choice, drink["ingredients"])
                


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:



