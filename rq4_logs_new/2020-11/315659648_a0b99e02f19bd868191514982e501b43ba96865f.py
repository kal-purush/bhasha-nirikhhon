# Amani Kashema class3
from tkinter import *
import random
from tkinter import messagebox as mb
import datetime as dt

root = Tk()
root.title("Validation")
root.geometry("500x500")
root.configure(bg="green")


class Lotto:
    def __init__(self):

        def login():
            try:
                if int(self.age.entry.get()) < 18:
                    mb.showwarning("Sorry","You do not qualify to play lotto")
            finally:
                if int(self.age.entry.get()) >= 18:
                    mb.showinfo("You qualify to play Lotto","Welcome to the Ithuba National Lottery of South Africa")
                    root.destroy()
                    root1 = Tk()
                    root1.title("Lotto App")
                    root1.geometry("500x500")
                    root1.configure(bg="brown")

                    def random_lotto():
                        input_list.append(int(a1.get()))
                        input_list.append(int(a2.get()))
                        input_list.append(int(a3.get()))
                        input_list.append(int(a4.get()))
                        input_list.append(int(a5.get()))
                        input_list.append(int(a6.get()))

                        lotto_numbers = sorted(random.sample(range(1,49),6))

                        if len(lotto_numbers) == len(input_list):
                            duplicates = set(lotto_numbers).intersection(set(input_list))
                            if len(duplicates) == 0:
                                answer.configure(text="You failed And the" + "Lucky Lotto Numbers are: " + str(lotto_numbers) + "\nPrize: R0")
                            elif len(duplicates) == 1:
                                answer.configure(text="Congratulations You got 1 number correct \n" + "Lucky Lotto Numbers are: " + str(lotto_numbers) + "\nPrize: R0")
                            elif len(duplicates) == 2:
                                answer.configure(text="Congratulations You got 2 numbers correct \n" + "Lucky Lotto Numbers are: " + str(lotto_numbers) + "\nPrize: R20" )
                            elif len(duplicates) == 3:
                                answer.configure(text="Congratulations You got 3 numbers correct \n" + "Lucky Lotto Numbers are: " + str(lotto_numbers) + "\nPrize: R110" )
                            elif len(duplicates) == 4:
                                answer.configure(text="Congratulations You got 4 numbers correct \n" + "Lucky Lotto Numbers are: " + str(lotto_numbers) + "\nPrize: R2384" )
                            elif len(duplicates) == 5:
                                answer.configure(text="Congratulations You got 5 numbers correct \n" + "Lucky Lotto Numbers are: " + str(lotto_numbers) + "\nPrize: R8584" )
                            elif len(duplicates) == 6:
                                answer.configure(text="Congratulations You got 6 numbers correct \n" + "Lucky Lotto Numbers are: " + str(lotto_numbers) + "\nPrice: R10 000 000" )

                    # lotto numbers
                    answer = Label(root1)
                    answer.place(x=40,y=200)
                    input_list = []

                    a1 = Entry(root1, width=4)
                    a1.place(x=0,y=10)

                    a2 = Entry(root1, width=4)
                    a2.place(x=50,y=10)

                    a3 = Entry(root1, width=4)
                    a3.place(x=100,y=10)

                    a4 = Entry(root1, width=4)
                    a4.place(x=150,y=10)

                    a5 = Entry(root1, width=4)
                    a5.place(x=200,y=10)

                    a6 = Entry(root1, width=4)
                    a6.place(x=250,y=10)

                    button = Button(root1,text="submit",command=random_lotto)
                    button.place(x=10,y=60)


        # label
        self.user_name = Label(root,text="Please enter your name")
        self.user_surname = Label(root,text="Please enter your Surname")
        self.age = Label(root,text="PLease enter your age")
        self.submit = Button(root,text="Check validation",command=login).place(x=50, y=100)
        # time label
        now = dt.datetime.now()
        self.time = Label(root)
        self.time.place(x=50, y=150)
        self.time.config(text="Date: " + str(now.strftime("%Y-%m-%d %H:%M:%S")))

        # entry
        self.user_name_entry = Entry(root).place(x=190, y=10)
        self.user_surname_entry = Entry(root).place(x=190, y=40)
        self.age.entry = Entry(root)

        # positioning
        self.user_name.place(x=0, y=10)
        self.user_surname.place(x=0, y=40)
        self.age.place(x=0, y=70)
        self.age.entry.place(x=190, y=70)


Lotto()
root.mainloop()