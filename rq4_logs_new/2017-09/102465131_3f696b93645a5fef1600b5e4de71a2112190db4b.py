# Mest is throwing a crazy networking
# party. they need us to write software to register guests.
# a. store guest(name,email) in a dictionary.
# b. given a name,output an email.
# c. send an email to all guest thanking them for showing.
# d. If the guest has an odd number of letters in their name, send them a different email, telling
# them they are unwelcome at future events.
import smtplib

class Guestlist:

    def __init__(self):
        self.guests = {}

    def add_guest(self, name, email):
        self.name = name
        self.email = email
        self.guests[self.name] = self.email
        return self.guests

    def send_mail(self):
        for name in self.guests.keys():
            if len(name) % 2 == 0:
                rtn = self.guests[name]
                message = "Thank You for attending the party."
                try:
                    server = smtplib.SMTP("smtp.gmail.com", 587)
                    server.starttls()
                    email = input("enter your email: ")
                    passw = input("enter your pass: ")
                    server.login(email, passw)
                    server.sendmail(email, rtn, message)
                    server.close()
                    print("Mail Sent")

                except Exception:
                    print("Error: couldn't send your mail")

            elif len(name) % 2 == 1:
                rtn = self.guests[name]
                message = "You are not welcome here please."
                try:
                    server = smtplib.SMTP("smtp.gmail.com", 587)
                    server.starttls()
                    email = input("enter your email: ")
                    passw = input("enter your pass: ")
                    server.login(email, passw)
                    print("your mail is processing")
                    server.sendmail(email, rtn, message)
                    server.close()

                    print("Mail Sent")
                except Exception as e:
                    print("Error: couldn't send your mail")


    def logic(self):
        #region me new region
        while True:
            choice = input("to add: '1'\nto search: '2' \n to sendmail:'3'  ")
            if choice == '1':
                new_name = input("enter a guest name: ")
                new_email = input("enter the guest email: ")
                self.add_guest(new_name, new_email)
                print("{} has been added to your list".format(new_name))
        #endregion
            elif choice == '2':
                new_name = input("enter a search name: ")
                if new_name in self.guests.keys():
                    rtn = self.guests[new_name]
                    print(rtn)

            elif choice == '3':
                self.send_mail()


            else:
                break
        return

fool = Guestlist()
fool.logic()






