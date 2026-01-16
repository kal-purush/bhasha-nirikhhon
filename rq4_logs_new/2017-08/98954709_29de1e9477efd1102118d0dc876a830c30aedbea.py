number_of_items = int(input("Number of items : "))
while number_of_items < 0:
    print("Invalid number of items")
    number_of_items = int(input("Number of items : "))

total_price = 0
for i in range(number_of_items):
    price_of_item = float(input("Price of item : "))
    total_price = total_price + price_of_item

if total_price > 100:
    discount = 0.9
else:
    discount = 1

dicount_price = total_price*discount
print("Total price for ", number_of_items, " items is $", "{:.2f}".format(dicount_price))