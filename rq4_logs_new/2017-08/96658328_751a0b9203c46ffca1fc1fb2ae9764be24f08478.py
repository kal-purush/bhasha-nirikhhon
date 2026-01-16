my_file = open("my_books_file.pkl", 'r')
for i in my_file.readlines():
    method = raw_input("Please enter the query method: \n1.Query the title \n2.Check by number ISBN \n3.Auther query \nPlease enter the query method:")
    if method == "1":
        enter = raw_input("Enter title:")
        if enter in i:
            print i
    elif method == "2":
        enter = raw_input("Enter ISBN number:")
        if enter in i:
            print i
    elif method == "3":
        enter = raw_input("Enter auther name:")
        if enter in i:
            print i 
        break