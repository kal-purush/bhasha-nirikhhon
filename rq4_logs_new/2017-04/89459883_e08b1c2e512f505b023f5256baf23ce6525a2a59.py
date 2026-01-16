import tkinter, sys, random
from tkinter import messagebox
random.seed()

def zmiana():
    global liczba
    liczba = random.randint(1, s.get())
    tekst = "Teraz zgadujsz z przedziału od 1 do "
    l2.config(text=tekst+str(s.get()))
def koniec():
    messagebox.showinfo(title="Uwaga", message="Na dziś kończymy")
    sys.exit()
def sprawdz():
    try:
        x = int(e.get())
        if x > liczba:
            l2.config(text="Moja liczba jest MNIEJSZA od twojej")
        elif x < liczba:
            l2.config(text="Moja liczba jest WIĘKSZA od twojej")
        else:
            l2.config(text="Zgadłeś!")
            messagebox.showinfo(title="Gratulacje", message="Wygrałeś! W nagrodę otrzymujesz nic ciekawego")
    except ValueError:
        messagebox.showinfo(title="Uwaga", message="Błędne dane na wejściu")
def nowaGra():
    global liczba
    liczba = random.randint(1, 101)
    l2.config(text="Nowa gra rozpoczęta")

liczba = random.randint(1,101)
x=0

main = tkinter.Tk()

e = tkinter.Entry(main, justify="center", width=50)
l = tkinter.Label(main, text = "Odgadnij liczbę z zakresu od 1 do 100 (lub zmień zakres)")
l2 = tkinter.Label(main, text="")
b = tkinter.Button(main, text = "Zakończ", command = koniec)
b2 = tkinter.Button(main, text = "Sprawdź", command = sprawdz)
b3 = tkinter.Button(main, text="Nowa gra", command=nowaGra)
l3 = tkinter.Label(main, text="Wybierz zakres od 1 do max 1000")
s = tkinter.Scale(main, orient="horizontal", from_=1, to_=1000, length=400)
b4 = tkinter.Button(main, text="zatwierdź zmianę zakresu i rozpocznij grę od nowa", command=zmiana)

l.pack()
l2.pack()
e.pack()
b2.pack()
b3.pack()
l3.pack()
s.pack()
b4.pack()
b.pack()

main.mainloop()