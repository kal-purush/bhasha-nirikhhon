import tkinter as tk
import winsound
import pickle

from game import Game
from game_window_components import *

#------------FUNCTIONS LEADING TO NEW WINDOWS--------------------
def music():
    winsound.PlaySound("faded", winsound.SND_ASYNC | winsound.SND_LOOP)
def new_game_but():
    game = Game()
def load_game_but():
    leader_window = tk.Toplevel()
    height = leader_window.winfo_screenheight()
    width = leader_window.winfo_screenwidth()
    leader_window.geometry("800x800+{}+{}".format((width - 100) // 4, height - height))
    leader_window.maxsize(1400, 1080)
    leader_window.minsize(550, 700)
    leader_window.title("BUBBLE SHOOTER--LEADERBOARDS")
    leader_window.columnconfigure(0, weight=1)
    leader_window.columnconfigure(1, weight=2)
    leader_window.columnconfigure(2, weight=1)
    leader_window.rowconfigure(0, weight=4)
    leader_window.rowconfigure(1, weight=1)
    leader_window.rowconfigure(2, weight=1)
    leader_window.rowconfigure(3, weight=1)

    # ---------------------------BACKGROUND IMAGE------------------------------
    tk.Label(leader_window, image=bg_image).place(relheight=1, relwidth=1)

    #---------- LOAD PROCESS------------
    leaderboard_list = tk.Listbox(leader_window, width=50, height=25)
    leaderboard_list.grid(row=4, column=1, pady=10, padx=30)
    leaderboard_list.insert("end", "                 -----SAVED GAMES WITH SCORE-------             ")
    with open('../docs/numberofsaves', "rb") as load_game:
        buffer1 = pickle.load(load_game)
        numberofsaves = buffer1
    for i in range(1,numberofsaves):
        with open(f'../docs/marbles{i}', "rb") as load_game:
            buffer1 = pickle.load(load_game)
            marbles = buffer1

        with open(f'../docs/score{i}', "rb") as load_game:
            buffer2 = pickle.load(load_game)
            score = int(buffer2)

        with open(f'../docs/name{i}', "rb") as load_game:
            buffer2 = pickle.load(load_game)
            name = buffer2
        leaderboard_list.insert("end", f"{name}                                                                         {score}")

        def anchor_checker(numberofsaves):
            for i in range(1,numberofsaves):
                with open(f'../docs/marbles{i}', "rb") as load_game:
                    buffer1 = pickle.load(load_game)
                    marbles = buffer1

                with open(f'../docs/score{i}', "rb") as load_game:
                    buffer2 = pickle.load(load_game)
                    score = int(buffer2)

                with open(f'../docs/name{i}', "rb") as load_game:
                    buffer2 = pickle.load(load_game)
                    name = buffer2
                holder=f"{name}                                                                         {score}"
                if leaderboard_list.get("anchor")==holder:
                    Game(marbles, score)


        # --------------------------------------CONTENT ----------------------------

    leaderboard_text = tk.Label(leader_window, text="PREVIOUS GAME", font="verdana 20 bold", bg="#ED2C5C",
                                height=3, width=25).grid(row=3, column=1, padx=160, sticky="s", pady=30)

    return_but = tk.Button(leader_window, text="BACK", bg="#ED2C5C", height=2, width=8, font="verdana 13 bold",
                           command=leader_window.destroy).grid(row=4, column=2)
    return_but = tk.Button(leader_window, text="CONTINUE", bg="#ED2C5C", height=2, width=14, font="verdana 13 bold",
                           command=lambda:anchor_checker(numberofsaves)).grid(row=3, column=2)
    leader_window.mainloop()
    #lambda: Game(marbles, score)


#-----------------SETTINGS BUTTON---------
def settings_but():


    #--------WRITES THE BUTTON ON TEXT FILE-------
    def callback1(value_holder="p"):
        if button1_var.get()!="":
            value_holder = button1_var.get()
        with open('../docs/settings.txt','w') as file:
            file.write("{}".format(value_holder))


    def callback2(value_holder="b"):
        if button2_var.get()!="":
            value_holder = button2_var.get()
        with open('../docs/settings.txt','a') as file:
            file.write("{}".format(value_holder))


    #----------SETTINGS WINDOW---------
    settings = tk.Toplevel()
    height = settings.winfo_screenheight()
    width = settings.winfo_screenwidth()
    settings.geometry("800x800+{}+{}".format((width - 100) // 4, height - height))
    settings.maxsize(1400, 1080)
    settings.minsize(550, 700)
    settings.title("BUBBLE SHOOTER--LEADERBOARDS")
    settings.columnconfigure(0, weight=1)
    settings.columnconfigure(1, weight=2)
    settings.columnconfigure(2, weight=1)
    for i in range(1,20):
        settings.rowconfigure(i,weight=1)


    tk.Label(settings, image=bg_image).place(relheight=1, relwidth=1)

    settings_text = tk.Label(settings, text="SETTINGS", font="verdana 20 bold", bg="#ED2C5C", height=2, width=13).grid(row=9, column=1, padx=10,sticky="s",pady=10)
    change_text = tk.Label( settings,text="Change the Pause key to your choice:", font="verdana 13 bold", bg="#ED2C5C").grid(row=10, column=1, pady=40)
    change1_text = tk.Label(settings,text="Change the Boss key to your choice:", font="verdana 13 bold", bg="#ED2C5C").grid( row=12, column=1, pady=40)
    button1_var = tk.StringVar()
    button2_var = tk.StringVar()
    entry1 = tk.Entry(settings, width=25, textvariable=button1_var).grid(row=11, column=1)
    entry2 = tk.Entry(settings, width=25, textvariable=button2_var).grid(row=13, column=1)
    button1 = tk.Button(settings, text="CHANGE", font="verdana 13 bold", bg="#ED2C5C", command=callback1).grid(row=11,  column=1,sticky="e",  padx=20)
    button2 = tk.Button(settings, text="CHANGE", font="verdana 13 bold", bg="#ED2C5C", command=callback2).grid(row=13,column=1, sticky="e",  padx=20)
    return_but = tk.Button(settings, text="BACK", bg="#ED2C5C", height=2, width=8, font="verdana 13 bold",
                           command=settings.destroy).grid(row=2, column=2)


    settings.mainloop()


#-----------------------LEADERBOARD WINDOW-------------------------------------
def leaderboard_but():

    leader_window = tk.Toplevel()
    height = leader_window.winfo_screenheight()
    width = leader_window.winfo_screenwidth()
    leader_window.geometry("800x800+{}+{}".format((width - 100) // 4, height - height))
    leader_window.maxsize(1400, 1080)
    leader_window.minsize(550, 700)
    leader_window.title("BUBBLE SHOOTER--LEADERBOARDS")
    leader_window.columnconfigure(0, weight=1)
    leader_window.columnconfigure(1, weight=2)
    leader_window.columnconfigure(2, weight=1)
    leader_window.rowconfigure(0,weight=3)
    leader_window.rowconfigure(1,weight=1)
    leader_window.rowconfigure(2,weight=1)
    leader_window.rowconfigure(3,weight=1)



    # ---------------------------BACKGROUND IMAGE------------------------------
    tk.Label(leader_window, image=bg_image).place(relheight=1, relwidth=1)

    # --------------------------------------CONTENT ----------------------------


    leaderboard_text = tk.Label(leader_window, text="LEADER BOARD", font="verdana 20 bold", bg="#ED2C5C",
                                height=3, width=25).grid(row=0,column=1, padx=160,sticky="s",pady=30)
    data = []
    with open('../docs/highscore.txt') as file:
        for line in file:
            try:
                name = line.strip().split()[0]
                score = int(line.strip().split()[1])
                data.append([name, score])
            except IndexError:
                pass
    cols = ('Position', 'Name', 'Score')
    listBox = ttk.Treeview(leader_window, columns=cols, show='headings')

    # set column headings
    for col in cols:
        listBox.heading(col, text=col)

    listBox.grid(row=1, column=0, columnspan=2)

    temp_list = data
    temp_list.sort(key=lambda e: e[1], reverse=True)

    for i, (name, score) in enumerate(temp_list, start=1):
        listBox.insert("", "end", values=(i, name, score))

    return_but = tk.Button(leader_window, text="BACK", bg="#ED2C5C", height=2, width=8, font="verdana 13 bold",
                           command=leader_window.destroy).grid(row=1, column=2)
    leader_window.mainloop()




#---------------------MAIN WINDOW---------------------
main_window=tk.Tk()
height = main_window.winfo_screenheight()
width = main_window.winfo_screenwidth()
main_window.geometry("800x800+{}+{}".format((width - 100) // 4, height - height))
main_window.maxsize(1400,1080)
main_window.minsize(550,700)
main_window.title("BUBBLE SHOOTER")
icon = tk.PhotoImage(file = "icon.png")
main_window.iconphoto(True,icon)

main_window.columnconfigure(0, weight=1)
main_window.columnconfigure(1, weight=2)
main_window.columnconfigure(2, weight=1)
music()



#--------------------BACKGROUND IMAGE---------------------------
bg_image=tk.PhotoImage(file="bgimage2.png")
bg_image=bg_image.subsample(5,5)
tk.Label(main_window,image=bg_image).place(relheight=1,relwidth=1)

#-----------------------SETTING UP BUTTONS FOR MAIN WINDOW----------------------------
but_frame=tk.Frame(main_window).grid(sticky="w",pady=120,padx=10)




new_game_button=tk.Button(but_frame,text="NEW GAME",font="comicsans 20 bold",bg="#ED2C5C",height=2,width=10,command=new_game_but)
new_game_button.grid(row=2,column=1,padx=12,pady=3)
Load_game_button=tk.Button(but_frame,text="LOAD GAME",font="comicsans 20 bold",bg="#ED2C5C",height=2,width=10,command=load_game_but)
Load_game_button.grid(row=3,column=1,padx=15,pady=3)
settings_button=tk.Button(but_frame,text="SETTINGS",font="comicsans 20 bold",bg="#ED2C5C",height=2,width=10,command=settings_but)
settings_button.grid(row=4,column=1,padx=15,pady=3)
leaderboard_button=tk.Button(but_frame,text="LEADER BOARD",font="comicsans 20 bold",bg="#ED2C5C",height=2,width=13,command=leaderboard_but)
leaderboard_button.grid(row=6,column=1,padx=15,pady=3)
quitt_button=tk.Button(but_frame,text="QUIT",font="comicsans 20 bold",bg="#ED2C5C",height=2,width=8,command=quit)
quitt_button.grid(row=7,column=1,padx=15,pady=3)


main_window.mainloop()