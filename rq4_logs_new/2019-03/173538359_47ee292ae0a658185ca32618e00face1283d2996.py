'''
This is the simple console app for electronic appliances shop.
for managing the stock and the save customer information.
Date: 2075/11/18
Author:Sachin Maharjan
'''
import pyfiglet
import stock as stk
import customer as cus

def main():
    #for welcoming user when opening this app for first time.

    store = pyfiglet.figlet_format("Sachin Electro")
    print(store)

    # login
    while True:
        print("Enter the password to login::")
        passWord = input("Enter the password::")
        if passWord == 'password':
            print("sucessfully login!")
            print("Welcome to Sachin Electro")
            break

    #main Driver part
    while True:

        print("\n__________________")
        print("*******MENU*******")
        print("__________________")
        print("1 : watching stock")
        print("2 : selling the product")
        print("3 : Watching customer information")
        print("4 : Increase the stock of existing product")
        print("5 : Add new Product to Inventory System")
        print("e : exiting from the app")

        choice = input("Enter your Choice:")
        if choice == '1':
            # showing the existing stock
            print("The following are exsiting stocks:")
            stk.showStock()

        elif choice == '2':
            # selling product
            customer = input("Enter the customer Name:")
            productName = input("Enter the product Name to sold:")
            totalAmount = stk.getPrice(productName)
            if totalAmount == 0:
                print("product not found!\n Cancel the sales!")
                continue;
            discountedAmount = input("Enter the discountedAmount:")
            cus.generateInvoice(customer,productName,discountedAmount,totalAmount)
            stk.decreaseStock(productName,1)


        elif choice == '3':
            print("The follwing are existing customer information::")
            cus.getCustomerInfo()

        elif choice == '4':
            # change the stock of existing product
            productName = input("Enter the product name to add Stock:")
            newStock = int(input("Enter the stock to be added on the old:"))
            stk.increaseStockInExisting(productName,newStock)

        elif choice == '5':

            #Adds new product to Inventory
            productName = input("Enter the name of new Inventory::")
            stock = int(input("Enter the stock of that product::"))
            pricePerUnit = int(input("Enter the price Per Unit of product::"))
            stk.newProductAdd(productName,stock,pricePerUnit)

        elif choice == 'e' or 'E':
            break

        else:
            print("Invalid Choice")

if __name__ == "__main__":
    main()