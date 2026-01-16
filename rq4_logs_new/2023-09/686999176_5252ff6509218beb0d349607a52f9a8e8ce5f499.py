import _datetime
def get_mini_statement(account_pin,account_number_mini_statement):
    a = 0
    acc_num = input("ENTER A BANK ACCOUNT NUMBER: ")
    if acc_num not in account_pin.keys():
        return "Invalid account number", 1

    usr_pin = input("ENTER A PIN: ")
    if usr_pin not in account_pin.values():
        return "Invalid pin", 1

    for i, j in account_pin.items():
        if i == acc_num and j == usr_pin:
            a += 1
            break
    if a > 0:
        pass
    else:
        return "Invalid information", 1

    for i,j in account_pin.items():
        if i==acc_num and j==usr_pin:
            d=account_number_mini_statement[i]
            print("="*50)
            print(d)
            print("=" * 50)
            return "yes",1

def account_money_transefer(account_balance,account_pin,account_number_mini_statement):
    a = 0
    acc_num = input("ENTER A BANK ACCOUNT NUMBER: ")
    if acc_num not in account_balance.keys():
        return "Invalid account number",1

    usr_pin = input("ENTER A PIN: ")
    if usr_pin not in account_pin.values():
        return "Invalid pin",1

    for i, j in account_pin.items():
        if i == acc_num and j == usr_pin:
            a += 1
            break
    if a > 0:
        pass
    else:
        return "Invalid information",1

    for i,j in account_balance.items():
        if i==acc_num:

            while True:
                enter_amount_transfer = int(input("enter amount to transfer:"))
                if enter_amount_transfer<=0:
                    print("ENTER A VALID AMOUNT")
                    continue
                else:
                    break

            if  enter_amount_transfer>j:
                return "insufficient funds",1
            else:
                transfer_account_number=input("enter the transfer account number: ")
                if transfer_account_number not in account_balance.keys():
                    return "Invalid transfer account number"
                else:
                    b=account_balance[i]
                    b=b-enter_amount_transfer
                    account_balance[i]=b
                    c=account_balance[transfer_account_number]
                    c=c+enter_amount_transfer
                    account_balance[transfer_account_number]=c
                    now = str(_datetime.datetime.now())
                    m = "YOUR TRANSFER AMOUNT IS :{}  DATE AND TIME: {}        ".format(enter_amount_transfer,now)
                    d = account_number_mini_statement[i]
                    account_number_mini_statement[i] = d + m

                    print("YOUR TRANSACTION IS SUCCESSFUL")
                    u_option=input("DO YOU WANT SHOW THE REMAINING BALANCE IN YOUR ACCOUNT YES/NO: ")
                    if u_option=="no" or u_option=="NO":
                        return account_balance,account_number_mini_statement
                    else:
                        print("YOUR ACCOUNT BALANCE IS: {}".format(b))
                        return account_balance,account_number_mini_statement











#cash deposit operaion
def cash_deposit_operation(accountno_phoneNumber,account_holder_name,account_balance,account_number_mini_statement):
    a = 0
    acc_num = input("ENTER A BANK ACCOUNT NUMBER: ")
    if acc_num not in account_balance.keys():
        return "Invalid account number",1
    user_mobile_number=input("ENTER REGISTERED MOBILE NUMBER :")
    if user_mobile_number not in accountno_phoneNumber.keys():
        return  "invalid mobile number",1
    for i,j in accountno_phoneNumber.items():
        if i==acc_num and j==user_mobile_number:
            a+=1
            break
    if a>0:
        pass
    else:
        return "Invalid account information",1

    for i,j in accountno_phoneNumber.items():
        if i==acc_num and j==user_mobile_number:
            name=account_holder_name[i]
            is_this_user=input("name: {} , account_number :{}   yes/no".format(name,i))
            if is_this_user=="no" or is_this_user=="NO":
                return "not my account",1
            else:
                amount=int(input("ENTER THE DEPOSIT AMOUNT: "))
                t=amount
                f_500=0
                h_100=0
                f_50=0
                t_10=0
                o_1=0
                if amount>=500:
                    b=amount//500
                    amount=amount-b*500
                    f_500=b
                if amount>=100:
                    b=amount//100
                    amount=amount-b*100
                    h_100=b
                if amount>=50:
                    b=amount//50
                    amount=amount-b*50
                    f_50=b
                if amount>=10:
                    b=amount//10
                    amount=amount-b*10
                    t_10=b
                if amount>=1:
                    o_1=amount
                tal=t
                print("500 : {}".format(f_500))
                print("100 : {}".format(h_100))
                print("50  : {}".format(f_50))
                print("10  : {}".format(t_10))
                print("1   : {}".format(o_1))
                print("="*50)
                print("TOTAL: {}".format(tal))
                u_input = input("PROCEED FOR NEXT STEP yes/no:")
                if u_input == "no" or u_input=="NO":
                    return "your money was not deposit",1
                else:
                    p_amount=account_balance[i]
                    account_balance[i]=p_amount+tal
                    print("YOUR AMOUNT IS SUCCESSFULLY DEPOSITED")
                    now = str(_datetime.datetime.now())
                    m = "YOUR DEPOSIT AMOUNT IS :{}  DATE AND TIME: {}        ".format(t, now)
                    d = account_number_mini_statement[i]
                    account_number_mini_statement[i]=d+m

                    u_input = input("DO YOU WANT SHOW BALANCE: yes/no")
                    if u_input == "no" or u_input == "NO":
                        return account_balance,account_number_mini_statement
                    else:
                        p=account_balance[i]
                        print("TOTAL AVAILABLE BALANCE IS :{}".format(p))
                        return account_balance,account_number_mini_statement



# amount withdraw operation

def amount_withdraw(account_balance,account_pin,account_number_mini_statement):
    a = 0
    acc_num = input("ENTER A BANK ACCOUNT NUMBER: ")
    if acc_num not in account_balance.keys():
        return "Invalid account number",1

    usr_pin = input("ENTER A PIN: ")
    if usr_pin not in account_pin.values():
        return "Invalid pin",1

    for i, j in account_pin.items():
        if i == acc_num and j == usr_pin:
            a += 1
            break
    if a > 0:
        pass
    else:
        return "Invalid information",1


    while True:
        amount = int(input("ENTER WITHDRAW AMOUNT : "))
        if amount<=0:
            print("PLEASE ENTER VALID AMOUNT")
        else:
            break


    for i,j in account_balance.items():
        if i==acc_num:
            if amount>j:
                return "insufficient funds",1
            else:
                re=j-amount
                account_balance[i]=re
                print("YOUR WITHDRAW AMOUNT IS :{}".format(amount))
                u_input=input("DO U WANT SHOW REMAINING AMOUNT yes/no:")
                now = str(_datetime.datetime.now())
                m="YOUR WITH DRAW AMOUNT IS :{}  DATE AND TIME: {}     ".format(amount,now)
                d=account_number_mini_statement[i]
                account_number_mini_statement[i]=d+m
                if u_input=="no" or u_input=="NO":

                    return account_balance,account_number_mini_statement
                else:
                    print("YOUR REMAINING AMOUNT IS: {}".format(re))
                    return account_balance,account_number_mini_statement







# display balance enquiry

def display_balance_enquiry(account_balance,account_pin):
    a=0
    acc_num = input("ENTER A BANK ACCOUNT NUMBER: ")
    if acc_num not in account_balance.keys():
        return "Invalid account number"

    usr_pin=input("ENTER A PIN: ")
    if usr_pin not in account_pin.values():
        return "Invalid pin"

    for i,j in account_pin.items():
        if i==acc_num and j==usr_pin:
            a+=1
            break
    if a>0:
        pass
    else:
        return "Invalid information"

    for i,j in account_pin.items():
        if i==acc_num and j==usr_pin:
            bal=account_balance[i]
            return bal





# atm card pin change

def atm_pin_change(account_pin):
    a = 0
    acc_num = input("ENTER A BANK ACCOUNT NUMBER: ")

    if acc_num not in account_pin.keys():
        return "Invalid account number"

    pre_pin=input("ENTER A PREVIOUS PIN: ")
    if pre_pin not in account_pin.values():
        return "Invalid pin"
    print(account_pin)
    for i,j in account_pin.items():
        if i==acc_num and j==pre_pin:
            a+=1
            break
    if a>0:

        while True:
            user_pin_input = input("ENTER A NEW PIN:")
            if user_pin_input.isdigit():
                if len(user_pin_input) < 5:
                    account_pin[acc_num] = user_pin_input
                    return account_pin
                else:
                    print("PLEASE ENTER A 4 DIGIT NUMBER: ")
                    continue
            else:
                print("PLEASE ENTER A 4 DIGIT NUMBER: ")
                continue
    else:
        return "Invalid account information"





def atm_pin_generation(account_pin,secret_person_pin):
    a=0
    acc_num=input("ENTER A BANK ACCOUNT NUMBER: ")
    scret_pin=int(input("ENTER SECRET PIN: "))

    for i,j in secret_person_pin.items():
        if i==acc_num and j==scret_pin:
            a+=1
            break
    if a>0:
        pass
    else:
        return "Invalid account number"
    while True:
        user_pin_input=input("ENTER A NEW PIN:")
        if user_pin_input.isdigit():
            if len(user_pin_input)<5:
                account_pin[acc_num]=user_pin_input
                return account_pin
            else:
                print("PLEASE ENTER A 4 DIGIT NUMBER: ")
                continue
        else:
            print("PLEASE ENTER A 4 DIGIT NUMBER: ")
            continue






# the below details are account information like data base
# COPY METHOD USING BECAUSE VALUES CHANGES BEFORE AND AFTER U SEE IT THERE

print("u can perform operation on below data")
print("who check ATM PIN GENERATING OPTION YOU MUST CHECK THE VALUES OF ACCOUNT NUMBER AND SECRET PIN AND PERFORM LAST ACCOUNT NUMBER BECAUSE NOT ADD THE ATM PIN ")
print("="*75)

accountno_phoneNumber={"ACCOUNT_NUMBER":"PHONE_NUMBER","1111111111":"8888888888","2222222222":"7777777777","3333333333":"6666666666","4444444444":"5555555555","5555555555":"4444444444","6666666666":"3333333333","7777777777":"2222222222","8888888888":"1111111111","9999999999":"0000000000"}
print("account number and linked mobile number")
print(accountno_phoneNumber)

account_holder_name={"ACCOUNT_NUMBER":"ACCOUNT_HOLDER_NAME","1111111111":"raja","2222222222":"mahesh","3333333333":"balu","4444444444":"hari reddy","5555555555":"mouli","6666666666":"varma","7777777777":"chiru","8888888888":"saketh","9999999999":"chaitanya"}
print("account number and name")
print(account_holder_name)

account_balance={"ACCOUNT_NUMBER":"ACCOUNT_BALANCE","1111111111":98765,"2222222222":5001,"3333333333":8987,"4444444444":98765,"5555555555":2345678,"6666666666":9876,"7777777777":2345,"8888888888":98765,"9999999999":1234}
print("account number and balance")
print(account_balance)

account_pin={"ACCOUNT_NUMBER":"ATM_PIN NUMBER","1111111111":"0101 ","2222222222":"2340","3333333333":"1010 ","4444444444":"4560","5555555555":"1100 ","6666666666":"6780","7777777777":"0011 ","8888888888":"8900","9999999999":" "}
print("account number and PIN")
print(account_pin)

secret_person_pin={"ACCOUNT_NUMBER":"SECRET PIN : WHO GENERATE ATM PIN","1111111111":1234,"2222222222":2345,"3333333333":3456,"4444444444":4567,"5555555555":5678,"6666666666":6789,"7777777777":7890,"8888888888":8901,"9999999999":9012}
print("account number and SECRET PIN")
print(secret_person_pin)

account_number_mini_statement={"ACCOUNT_NUMBER":"MINI_STATEMENT","1111111111":" ","2222222222":" ","3333333333":" ","4444444444":" ","5555555555":" ","6666666666":" ","7777777777":" ","8888888888":" ","9999999999":" "}
print("account number and mini statement")
print(account_number_mini_statement)



while True:
    print("WELCOME TO SBI ATM ,  THANKS FOR VISITING US,  HOW CAN I HELP YOU")
    print("="*50)
    print("1. ATM PIN GENERATION")
    print("2. ATM PIN CHANGE")
    print("3. BALANCE ENQUIRY")
    print("4. CASH WITHDRAW")
    print("5. CARD LESS CASH DEPOSIT")
    print("6. MONEY TRANSFER TO ACCOUNT NUMBER")
    print("7. MINI STATEMENT")
    print("8. SHOPPING PAYMENT")
    print("=" * 50)

    while True:
        u_input = input("ENTER NUMBER: ")
        if (u_input == "1" or u_input=="2" or u_input=="3" or u_input=="4" or u_input=="5" or u_input=="6" or u_input=="7"):
            break
        print("PLEASE ENTER RIGHT NUMBER")
        print("=" * 50)


# atm pin generating function

    if u_input == "1":
        result = atm_pin_generation (account_pin,secret_person_pin)
        if result=="Invalid account number":
            print("INVALID ACCOUNT NUMBER")
            print("PLEASE TRY AGAIN")
            u_result=input("DO YOU WANT SHOW MAIN MENU:yes/no: ")
            if u_result=="no" or u_result=="NO":
                print("THANKS FOR VISITING US")
                print("="*50)
                break
            else:
                continue

        elif type(result)==dict:
            account_pin=result
            print("YOUR PIN GENERATING IS SUCCESSFULLY")
            u_taken_input=input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input=="no" or u_taken_input=="NO":
                print("THANKS FOR VISITING US")
                print(account_pin)
                break
            else:
                print("="*50)
                print(account_pin)
                continue

# atm pin change function

    elif u_input == "2":

        result = atm_pin_change(account_pin)
        if result=="Invalid account number":
            print("INVALID ACCOUNT NUMBER")
            print("PLEASE TRY AGAIN")
            u_result = input("DO YOU WANT SHOW MAIN MENU :yes/no: ")
            if u_result == "no" or u_result=="NO":
                print("THANKS FOR VISITING US")
                print("=" * 50)
                break
            else:
                continue
        elif result=="Invalid pin":
            print("INVALID PIN NUMBER")
            print("PLEASE TRY AGAIN")
            u_result = input("DO YOU WANT SHOW MAIN MENU :yes/no: ")
            if u_result == "no" or u_result=="NO":
                print("THANKS FOR VISITING US")
                print("=" * 50)
                break
            else:
                continue
        elif result=="Invalid account information":
            print("INVALID ACCOUNT INFORMATION")
            print("PLEASE TRY AGAIN")
            u_result = input("DO YOU WANT SHOW MAIN MENU :yes/no: ")
            if u_result == "no" or u_result=="NO":
                print("THANKS FOR VISITING US")
                print("=" * 50)
                break
            else:
                continue
        elif type(result)==dict:
            account_pin=result
            print("YOUR PIN GENERATING IS SUCCESSFULLY")
            u_taken_input=input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input=="no" or u_taken_input =="NO":
                print("THANKS FOR VISITING US")
                print(account_pin)
                break
            else:
                print("="*50)
                print(account_pin)
                continue



    elif u_input == "3":
        result=display_balance_enquiry(account_balance,account_pin)
        if result=="Invalid account number":
            print("INVALID ACCOUNT NUMBER")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input=="no" or u_taken_input =="NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("="*50)
                continue
        elif result=="Invalid pin":
            print("INVALID PIN NUMBER")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input =="NO":
                break
            else:
                print("=" * 50)
                continue
        elif result=="Invalid information":
            print("INVALID ACCOUNT INFORMATION")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input =="NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue
        else:
            print("YOUR ACCOUNT BALANCE IS: {}".format(result))
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input =="NO":
                break
            else:
                print("=" * 50)
                continue

    elif u_input == "4":

        result,mini=amount_withdraw(account_balance,account_pin,account_number_mini_statement)
        if result=="Invalid account number":
            print("INVALID ACCOUNT NUMBER")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input=="no" or u_taken_input =="NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("="*50)
                continue
        elif result=="Invalid pin":
            print("INVALID PIN NUMBER")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input =="NO":
                break
            else:
                print("=" * 50)
                continue
        elif result=="Invalid information":
            print("INVALID ACCOUNT INFORMATION")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input =="NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue

        elif result=="insufficient funds":
            print("INSUFFICIENT FUNDS")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input =="NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue
        elif type(result)==dict:
            account_balance=result
            account_number_mini_statement=mini
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input=="NO":
                print("THANKS FOR VISITING US")
                print(account_balance)
                print(account_number_mini_statement)
                break
            else:

                print(account_balance)
                print(account_number_mini_statement)
                print("=" * 50)
                continue

    elif u_input == "5":

        result,mini=cash_deposit_operation(accountno_phoneNumber,account_holder_name,account_balance,account_number_mini_statement)
        if result=="Invalid account number":
            print("INVALID ACCOUNT NUMBER")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue
        elif result=="invalid mobile number":
            print("INVALID PHONE NUMBER")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue
        elif result=="Invalid account information":
            print("INVALID ACCOUNT INFORMATION")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue
        elif result=="not my account":
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue
        elif result=="your money was not deposit":
            print("YOUR MONEY WAS NOT DEPOSIT")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue
        elif type(result)==dict:

            account_balance=result
            account_number_mini_statement = mini

            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                print(account_balance)
                print(account_number_mini_statement)
                break
            else:
                print(account_balance)
                print(account_number_mini_statement)
                print("=" * 50)
                continue


    elif u_input == "6":

        result,mini=account_money_transefer(account_balance,account_pin,account_number_mini_statement)
        if result=="Invalid account number":
            print("INVALID ACCOUNT NUMBER")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue
        elif result=="Invalid pin":
            print("INVALID PIN NUMBER")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                break
            else:
                print("=" * 50)
                continue
        elif result=="Invalid information":
            print("INVALID ACCOUNT INFORMATION")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue
        elif result=="insufficient funds":
            print("INSUFFICIENT FUNDS TO TRANSFER")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue
        elif result=="Invalid transfer account number":
            print("TRANSFER ACCOUNT NUMBER IS WRONG")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue

        elif type(result)==dict:
            account_number_mini_statement=mini
            account_balance = result

            print("YOUR AMOUNT IS SUCCESSFULLY DEPOSITED")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                print(account_balance)
                print(account_number_mini_statement)
                break
            else:
                print(account_balance)
                print(account_number_mini_statement)
                print("=" * 50)
                continue


    elif u_input == "7":
        result,mini=get_mini_statement(account_pin,account_number_mini_statement)
        if result=="Invalid account number":
            print("INVALID ACCOUNT NUMBER")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue
        elif result=="Invalid pin":
            print("INVALID PIN NUMBER")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                break
            else:
                print("=" * 50)
                continue
        elif result=="Invalid information":
            print("INVALID ACCOUNT INFORMATION")
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue
        elif result=="yes":
            u_taken_input = input("DO YOU WANT SHOW MAIN MENU: yes/no :")
            if u_taken_input == "no" or u_taken_input == "NO":
                print("THANKS FOR VISITING US")
                break
            else:
                print("=" * 50)
                continue





