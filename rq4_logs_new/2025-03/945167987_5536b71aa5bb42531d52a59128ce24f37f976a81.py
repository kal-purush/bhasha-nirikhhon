import pandas as pd
import tkinter as tk
from PIL import ImageTk, Image
from tkinter import messagebox
import random
import matplotlib.pyplot as plt
import openpyxl
class Player: #A class was created for Pokemon selection.
    def __init__(self, player_name, pokemon_list):
        self.player_name = player_name
        self.window = tk.Tk() # Window created
        self.window.title("Pokemon") #The title of the window is set
        self.frame1 = tk.Frame(self.window) #Frame1 created
        self.frame1.pack(side=tk.TOP) #The location of the frame1 was determined.
        self.frame2 = tk.Frame(self.window) #Frame2 created
        self.frame2.pack(side=tk.RIGHT) #The location of the frame2 was determined.
        self.frame3 = tk.Frame(self.window) #Frame3 created
        self.frame3.pack(side=tk.LEFT) #The location of the frame3 was determined.

        self.default_image = ImageTk.PhotoImage(Image.open("none.png")) # none.png was set as the default image.

        self.df = pd.read_csv('cleanPokemons.csv') #The cleanPokemon file has been read.

        self.Label1 = tk.Label(self.frame1, text=f"{player_name} chooses Pokemon!") #Label has been created. The text of the label varies depending on the player name.
        self.Label1.pack(side=tk.TOP, anchor=tk.CENTER) #The location of the label has been determined

        self.Label2 = tk.Label(self.frame2, image=self.default_image) #The label to upload the images has been created.
        self.Label2.pack(side=tk.TOP, anchor=tk.CENTER) #The location of the label has been determined

        self.button = tk.Button(self.frame2, text="Choose!", command=self.on_choose_button_click) #A button has been created to select Pokemon.
        self.button.pack(side=tk.BOTTOM) #The location of the button has been determined

        self.listbox = tk.Listbox(self.frame3, selectmode=tk.SINGLE) #A listbox containing the names of Pokemon has been created.
        self.listbox.pack(side=tk.LEFT, anchor=tk.CENTER) #The location of the listbox has been determined

        for name in pokemon_list: #A list with the names of Pokemon is defined in the listbox.
            self.listbox.insert(tk.END, name)

        self.listbox.bind("<<ListboxSelect>>", self.choose_pokemon)
        self.selected_pokemon_data = None #Created for selected pokemon information.
        self.selected_pokemon_image = None #Created for the picture of the selected pokemon.
        self.player_health = 0 #Specifies the pokemon's health value.
        self.player_attack = 0 #Specifies the attack value of the pokemon.
        self.player_element = "" #Created for the element of pokemon.
        self.health_history = []  # Created a list for health status history after each hit.
        self.attack_history = []  # A list has been created to record each hit.
        self.score = 0  # Added score variable

        self.window.mainloop() #It was written to create the window.

    def choose_pokemon(self, event=None): #It is a function written to select Pokemon and access information about the selected Pokemon.
        selected_index = self.listbox.curselection()
        if selected_index:
            selected_pokemon = self.df[self.df['Name'] == self.listbox.get(selected_index[0])]
            self.selected_pokemon_name = selected_pokemon.iloc[0]['Name']
            self.selected_pokemon_data = selected_pokemon.iloc[0]
            pokemon_image_path = f"{selected_pokemon.iloc[0]['Name']}.png" #It is written to access the image file of the pokemon selected in the listbox.
            self.selected_pokemon_image = ImageTk.PhotoImage(Image.open(pokemon_image_path))
            self.Label2.config(image=self.selected_pokemon_image) #The relevant label was created to identify the picture of the selected Pokemon.
            self.player_health = 5*int(selected_pokemon.iloc[0]['HP']) #The health value of the selected Pokemon is reached and the health value is multiplied by five.
            self.player_attack = int(selected_pokemon.iloc[0]['Attack']) #It was written to reach the attack value of the selected Pokemon.
            self.player_element = selected_pokemon.iloc[0]['Element'] #It was written to reach the Element of the selected Pokemon.

    def on_choose_button_click(self): #It is a function written to close the window.
        self.window.destroy()

    def increase_score(self): #It is a function created to keep track of players' scores.
        self.score += 1 #Increases the score by 1
        if self.score == 3: #A message box appears for the first player whose score is 3, stating that he has won.
            messagebox.showinfo(title="Game Over!", message=f"{self.player_name} wins the round! The game will restart.")


class BothPlayers:
    def __init__(self, player1, player2, df):
        self.player1 = player1 #player1 defined
        self.player2 = player2 #player2 defined
        self.current_player = player1 #A variable has been created indicating that the first player to start the game is player1.
        self.df = df
        self.window = tk.Tk() # Window created
        self.window.title("Pokemon") #The title of the window is set
        self.frame1 = tk.Frame(self.window) #Frame1 created
        self.frame1.pack(side=tk.LEFT) #The location of the frame1 was determined.
        self.frame2 = tk.Frame(self.window) #Frame2 created
        self.frame2.pack(side=tk.RIGHT) #The location of the frame2 was determined.
        self.default_image = ImageTk.PhotoImage(Image.open("none.png")) # none.png was set as the default image.
        self.Critical_count = {'Player 1': 0, 'Player 2': 0} #It is created for the number of critical hits made by players.
        self.battle_data = [] #A list that collects the necessary information for the excel file to be created at the end of the game.
        self.update_gui() #It calls the update_gui function.

    def update_gui(self):
        self.label_player1 = tk.Label(self.frame1, text=self.player1.player_name) #A label showing the player1 name has been created.
        self.label_player1.pack(side=tk.TOP) #The location of the label_player1 was determined.
        self.label2_player1 = tk.Label(self.frame1, text=f"Score: {self.player1.score}") #A label was created to show Player1's score.
        self.label2_player1.pack(side=tk.TOP)  #The location of the label2_player1 was determined.
        self.frame3_player1 = tk.Frame(self.frame1) #Frame created
        self.frame3_player1.pack(side=tk.TOP) #The location of the frame3_player1 was determined.
        self.label_health_player1 = tk.Label(self.frame3_player1, bg="green", width=int(self.player1.player_health)//10) #A label was created to show the current health of the pokemon.
        self.label_health_player1.pack(side=tk.LEFT) #The location of the label_health_player1 was determined.
        self.label_damage_player1 = tk.Label(self.frame3_player1, bg="red") #A label has been created to show the damage received by the Pokemon.
        self.label_damage_player1.pack(side=tk.LEFT) #The location of the label_damage_player1 was determined.
        self.label3_player1 = tk.Label(self.frame3_player1, text=f"{self.player1.player_health}/{5*self.player1.selected_pokemon_data['HP']}") #Label that will show pokemon's health value as a number
        self.label3_player1.pack(side=tk.LEFT) #The location of the label3_player1 was determined.
        self.player1_name = tk.Label(self.frame1, text=f"{self.player1.selected_pokemon_name}") #Label showing the name of the pokemon chosen by player1
        self.player1_name.pack(side=tk.TOP) #The location of the player1_name was determined.
        self.label_image_player1 = tk.Label(self.frame1, image=self.default_image) #Label showing the picture of the pokemon chosen by player1
        self.label_image_player1.pack(side=tk.TOP) #The location of the label_image_player1 was determined.
        self.button_physical_player1 = tk.Button(self.frame1, text="Physical", command=self.physical_attack_player1, state=tk.NORMAL if self.player1.score < 3 else tk.DISABLED)
        self.button_physical_player1.pack(side=tk.LEFT) #A physical attack button has been created for player1.
        self.button_elemental_player1 = tk.Button(self.frame1, text="Elemental", command=self.elemental_attack_player1, state=tk.NORMAL if self.player1.score < 3 else tk.DISABLED)
        self.button_elemental_player1.pack(side=tk.LEFT) #A elemental attack button has been created for player1.

        self.label_player2 = tk.Label(self.frame2, text=self.player2.player_name) #A label showing the player2 name has been created.
        self.label_player2.pack(side=tk.TOP) #The location of the label_player2 was determined.
        self.label2_player2 = tk.Label(self.frame2, text=f"Score: {self.player2.score}") #A label was created to show Player2's score.
        self.label2_player2.pack(side=tk.TOP)  #The location of the label2_player2 was determined.
        self.frame3_player2 = tk.Frame(self.frame2) #Frame created
        self.frame3_player2.pack(side=tk.TOP) #The location of the frame3_player2 was determined.
        self.label_health_player2 = tk.Label(self.frame3_player2, bg="green", width=int(self.player2.player_health)//10) #A label was created to show the current health of the pokemon.
        self.label_health_player2.pack(side=tk.LEFT) #The location of the label_health_player2 was determined.
        self.label_damage_player2 = tk.Label(self.frame3_player2, bg="red") ##A label has been created to show the damage received by the Pokemon.
        self.label_damage_player2.pack(side=tk.LEFT) #The location of the label_damage_player2 was determined.
        self.label3_player2 = tk.Label(self.frame3_player2, text=f"{self.player2.player_health}/{5*self.player2.selected_pokemon_data['HP']}") #Label that will show pokemon's health value as a number
        self.label3_player2.pack(side=tk.LEFT) #The location of the label3_player2 was determined.
        self.player2_name = tk.Label(self.frame2, text =f"{self.player2.selected_pokemon_name}") #Label showing the name of the pokemon chosen by player2
        self.player2_name.pack(side=tk.TOP) #The location of the player2_name was determined.
        self.label_image_player2 = tk.Label(self.frame2, image=self.default_image) #Label showing the picture of the pokemon chosen by player2
        self.label_image_player2.pack(side=tk.TOP) #The location of the label_image_player2 was determined.
        self.button_physical_player2 = tk.Button(self.frame2, text="Physical", command=self.physical_attack_player2, state=tk.DISABLED)
        self.button_physical_player2.pack(side=tk.LEFT) #A physical attack button has been created for player2. Since the 1st player will start the game, the 2nd player's buttons are deactivated.
        self.button_elemental_player2 = tk.Button(self.frame2, text="Elemental", command=self.elemental_attack_player2, state=tk.DISABLED)
        self.button_elemental_player2.pack(side=tk.LEFT) #A elemental attack button has been created for player2. Since the 1st player will start the game, the 2nd player's buttons are deactivated.


        self.window.mainloop()

    def switch_turn(self): #It is a function created for players to play in turns.
        if self.current_player == self.player1: #When it's player1's turn to move
            self.button_physical_player2["state"] = tk.DISABLED #Player 1's buttons are active.
            self.button_elemental_player2["state"] = tk.DISABLED #Player 1's buttons are active.
            self.button_physical_player1["state"] = tk.NORMAL #The 2nd player's buttons are deactivated.
            self.button_elemental_player1["state"] = tk.NORMAL #The 2nd player's buttons are deactivated.
            self.current_player = self.player2  #The turn to move passes to the 2nd player.

        elif self.current_player == self.player2: #When it's player2's turn to move
            self.button_physical_player1["state"] = tk.DISABLED #Player 1's buttons are deactive.
            self.button_elemental_player1["state"] = tk.DISABLED #Player 1's buttons are deactive.
            self.button_physical_player2["state"] = tk.NORMAL #Player 2's buttons are active.
            self.button_elemental_player2["state"] = tk.NORMAL #Player 2's buttons are active.
            self.current_player = self.player1  # The turn to move passes to the first player.

        if self.player1.player_health <= 0: #If playe1's health is 0 or below 0, the end_game function is called.
            self.end_game(self.player2)
        elif self.player2.player_health <= 0: #If playe2's health is 0 or below 0, the end_game function is called.
            self.end_game(self.player1)

    def end_game(self, winner):
        winner.increase_score()  # Winner's score increased
        if winner == self.player1: #Written for the situation where player 1 is the winner.
            if self.player1.selected_pokemon_name == "Bulbasaur": #Written for level development
                self.player1.selected_pokemon_name = "Ivysaur"
                self.player1_name.config(text=f"{self.player1.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player1.selected_pokemon_data = self.df[self.df["Name"] == self.player1.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player1.player_health = (5 * int(self.player1.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used.
                self.player2.player_health = 5 * int(self.player2.selected_pokemon_data['HP']) #The other Pokemon's health is brought to its maximum value.
                self.update_health_bars() #The update_health_bars function is called.
            elif self.player1.selected_pokemon_name == "Ivysaur":  #Written for level development
                self.player1.selected_pokemon_name = "Venusaur"
                self.player1_name.config(text=f"{self.player1.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player1.selected_pokemon_data = self.df[self.df["Name"] == self.player1.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player1.player_health = (5 * int(self.player1.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used.
                self.player2.player_health = 5 * int(self.player2.selected_pokemon_data['HP'])#The other Pokemon's health is brought to its maximum value.
                self.update_health_bars() #The update_health_bars function is called.
            elif self.player1.selected_pokemon_name == "Charmander": #Written for level development
                self.player1.selected_pokemon_name = "Charmeleon"
                self.player1_name.config(text=f"{self.player1.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player1.selected_pokemon_data = self.df[self.df["Name"] == self.player1.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player1.player_health = (5 * int(self.player1.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used.
                self.player2.player_health = 5 * int(self.player2.selected_pokemon_data['HP']) #The other Pokemon's health is brought to its maximum value.
                self.update_health_bars() #The update_health_bars function is called.
            elif self.player1.selected_pokemon_name == "Charmeleon": #Written for level development
                self.player1.selected_pokemon_name = "Charizard"
                self.player1_name.config(text=f"{self.player1.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player1.selected_pokemon_data = self.df[self.df["Name"] == self.player1.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player1.player_health = (5 * int(self.player1.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used.
                self.player2.player_health = 5 * int(self.player2.selected_pokemon_data['HP']) #The other Pokemon's health is brought to its maximum value.
                self.update_health_bars() #The update_health_bars function is called.
            elif self.player1.selected_pokemon_name == "Squirtle": #Written for level development
                self.player1.selected_pokemon_name = "Wartortle"
                self.player1_name.config(text=f"{self.player1.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player1.selected_pokemon_data = self.df[self.df["Name"] == self.player1.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player1.player_health = (5 * int(self.player1.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used.
                self.player2.player_health = 5 * int(self.player2.selected_pokemon_data['HP']) #The other Pokemon's health is brought to its maximum value.
                self.update_health_bars() #The update_health_bars function is called.
            elif self.player1.selected_pokemon_name == "Wartortle":#Written for level development
                self.player1.selected_pokemon_name = "Blastoise"
                self.player1_name.config(text=f"{self.player1.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player1.selected_pokemon_data = self.df[self.df["Name"] == self.player1.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player1.player_health = (5 * int(self.player1.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used.
                self.player2.player_health = 5 * int(self.player2.selected_pokemon_data['HP']) #The other Pokemon's health is brought to its maximum value.
                self.update_health_bars() #The update_health_bars function is called.
            elif self.player1.selected_pokemon_name == "Caterpie":#Written for level development
                self.player1.selected_pokemon_name = "Metapod"
                self.player1_name.config(text=f"{self.player1.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player1.selected_pokemon_data = self.df[self.df["Name"] == self.player1.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player1.player_health = (5 * int(self.player1.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used.
                self.player2.player_health = 5 * int(self.player2.selected_pokemon_data['HP']) #The other Pokemon's health is brought to its maximum value.
                self.update_health_bars() #The update_health_bars function is called.
            elif self.player1.selected_pokemon_name == "Metapod": #Written for level development
                self.player1.selected_pokemon_name = "Butterfree"
                self.player1_name.config(text=f"{self.player1.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player1.selected_pokemon_data = self.df[self.df["Name"] == self.player1.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player1.player_health = (5 * int(self.player1.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used.
                self.player2.player_health = 5 * int(self.player2.selected_pokemon_data['HP']) #The other Pokemon's health is brought to its maximum value.
                self.update_health_bars() #The update_health_bars function is called.
            elif self.player1.selected_pokemon_name == "Weedle": #Written for level development
                self.player1.selected_pokemon_name = "Kakuna"
                self.player1_name.config(text=f"{self.player1.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player1.selected_pokemon_data = self.df[self.df["Name"] == self.player1.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player1.player_health = (5 * int(self.player1.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used
                self.player2.player_health = 5 * int(self.player2.selected_pokemon_data['HP']) #The other Pokemon's health is brought to its maximum value.
                self.update_health_bars() #The update_health_bars function is called.
            elif self.player1.selected_pokemon_name == "Kakuna": #Written for level development
                self.player1.selected_pokemon_name = "Beedrill"
                self.player1_name.config(text=f"{self.player1.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player1.selected_pokemon_data = self.df[self.df["Name"] == self.player1.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player1.player_health = (5 * int(self.player1.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used
                self.player2.player_health = 5 * int(self.player2.selected_pokemon_data['HP']) #The other Pokemon's health is brought to its maximum value.
                self.update_health_bars() #The update_health_bars function is called.
            elif self.player1.selected_pokemon_name == "Pidgey": #Written for level development
                self.player1.selected_pokemon_name = "Pidgeotto"
                self.player1_name.config(text=f"{self.player1.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player1.selected_pokemon_data = self.df[self.df["Name"] == self.player1.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player1.player_health = (5 * int(self.player1.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used
                self.player2.player_health = 5 * int(self.player2.selected_pokemon_data['HP']) #The other Pokemon's health is brought to its maximum value.
                self.update_health_bars() #The update_health_bars function is called.
            elif self.player1.selected_pokemon_name == "Pidgeotto": #Written for level development
                self.player1.selected_pokemon_name = "Pidgeot"
                self.player1_name.config(text=f"{self.player1.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player1.selected_pokemon_data = self.df[self.df["Name"] == self.player1.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player1.player_health = (5 * int(self.player1.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used
                self.player2.player_health = 5 * int(self.player2.selected_pokemon_data['HP']) #The other Pokemon's health is brought to its maximum value.
                self.update_health_bars()#The update_health_bars function is called.

        if winner == self.player2: #Written for the situation where player 1 is the winner.
            if self.player2.selected_pokemon_name == "Bulbasaur": #Written for level development
                self.player2.selected_pokemon_name = "Ivysaur"
                self.player2_name.config(text=f"{self.player2.selected_pokemon_name}") #The name of the new pokemon appears on the label.
                self.player2.selected_pokemon_data = self.df[self.df["Name"] == self.player2.selected_pokemon_name].iloc[0] #Information about the selected Pokemon is accessed.
                self.player2.player_health = (5 * int(self.player2.selected_pokemon_data['HP'])) * 0.7 #Seventy percent of the evolved Pokemon's health is used
                self.player1.player_health = 5 * int(self.player1.selected_pokemon_data['HP']) #The other Pokemon's health is brought to its maximum value.
                self.update_health_bars() #The update_health_bars function is called.
            elif self.player2.selected_pokemon_name == "Ivysaur":
                self.player2.selected_pokemon_name = "Venusaur"
                self.player2_name.config(text=f"{self.player2.selected_pokemon_name}")
                self.player2.selected_pokemon_data = self.df[self.df["Name"] == self.player2.selected_pokemon_name].iloc[0]
                self.player2.player_health = (5 * int(self.player2.selected_pokemon_data['HP'])) * 0.7
                self.player1.player_health = 5 * int(self.player1.selected_pokemon_data['HP'])
                self.update_health_bars()
            elif self.player2.selected_pokemon_name == "Charmander":
                self.player2.selected_pokemon_name = "Charmeleon"
                self.player2_name.config(text=f"{self.player2.selected_pokemon_name}")
                self.player2.selected_pokemon_data = self.df[self.df["Name"] == self.player2.selected_pokemon_name].iloc[0]
                self.player2.player_health = (5 * int(self.player2.selected_pokemon_data['HP'])) * 0.7
                self.player1.player_health = 5 * int(self.player1.selected_pokemon_data['HP'])
                self.update_health_bars()
            elif self.player2.selected_pokemon_name == "Charmeleon":
                self.player2.selected_pokemon_name = "Charizard"
                self.player2_name.config(text=f"{self.player2.selected_pokemon_name}")
                self.player2.selected_pokemon_data = self.df[self.df["Name"] == self.player2.selected_pokemon_name].iloc[0]
                self.player2.player_health = (5 * int(self.player2.selected_pokemon_data['HP'])) * 0.7
                self.player1.player_health = 5 * int(self.player1.selected_pokemon_data['HP'])
                self.update_health_bars()
            elif self.player2.selected_pokemon_name == "Squirtle":
                self.player2.selected_pokemon_name = "Wartortle"
                self.player2_name.config(text=f"{self.player2.selected_pokemon_name}")
                self.player2.selected_pokemon_data = self.df[self.df["Name"] == self.player2.selected_pokemon_name].iloc[0]
                self.player2.player_health = (5 * int(self.player2.selected_pokemon_data['HP'])) * 0.7
                self.player1.player_health = 5 * int(self.player1.selected_pokemon_data['HP'])
                self.update_health_bars()
            elif self.player2.selected_pokemon_name == "Wartortle":
                self.player2.selected_pokemon_name = "Blastoise"
                self.player2_name.config(text=f"{self.player2.selected_pokemon_name}")
                self.player2.selected_pokemon_data = self.df[self.df["Name"] == self.player2.selected_pokemon_name].iloc[0]
                self.player2.player_health = (5 * int(self.player2.selected_pokemon_data['HP'])) * 0.7
                self.player1.player_health = 5 * int(self.player1.selected_pokemon_data['HP'])
                self.update_health_bars()
            elif self.player2.selected_pokemon_name == "Caterpie":
                self.player2.selected_pokemon_name = "Metapod"
                self.player2_name.config(text=f"{self.player2.selected_pokemon_name}")
                self.player2.selected_pokemon_data = self.df[self.df["Name"] == self.player2.selected_pokemon_name].iloc[0]
                self.player2.player_health = (5 * int(self.player2.selected_pokemon_data['HP'])) * 0.7
                self.player1.player_health = 5 * int(self.player1.selected_pokemon_data['HP'])
                self.update_health_bars()
            elif self.player2.selected_pokemon_name == "Metapod":
                self.player2.selected_pokemon_name = "Butterfree"
                self.player2_name.config(text=f"{self.player2.selected_pokemon_name}")
                self.player2.selected_pokemon_data = self.df[self.df["Name"] == self.player2.selected_pokemon_name].iloc[0]
                self.player2.player_health = (5 * int(self.player2.selected_pokemon_data['HP'])) * 0.7
                self.player1.player_health = 5 * int(self.player1.selected_pokemon_data['HP'])
                self.update_health_bars()
            elif self.player2.selected_pokemon_name == "Weedle":
                self.player2.selected_pokemon_name = "Kakuna"
                self.player2_name.config(text=f"{self.player2.selected_pokemon_name}")
                self.player2.selected_pokemon_data = self.df[self.df["Name"] == self.player2.selected_pokemon_name].iloc[0]
                self.player2.player_health = (5 * int(self.player2.selected_pokemon_data['HP'])) * 0.7
                self.player1.player_health = 5 * int(self.player1.selected_pokemon_data['HP'])
                self.update_health_bars()
            elif self.player2.selected_pokemon_name == "Kakuna":
                self.player2.selected_pokemon_name = "Beedrill"
                self.player2_name.config(text=f"{self.player2.selected_pokemon_name}")
                self.player2.selected_pokemon_data = self.df[self.df["Name"] == self.player2.selected_pokemon_name].iloc[0]
                self.player2.player_health = (5 * int(self.player2.selected_pokemon_data['HP'])) * 0.7
                self.player1.player_health = 5 * int(self.player1.selected_pokemon_data['HP'])
                self.update_health_bars()
            elif self.player2.selected_pokemon_name == "Pidgey":
                self.player2.selected_pokemon_name = "Pidgeotto"
                self.player2_name.config(text=f"{self.player2.selected_pokemon_name}")
                self.player2.selected_pokemon_data = self.df[self.df["Name"] == self.player2.selected_pokemon_name].iloc[0]
                self.player2.player_health = (5 * int(self.player2.selected_pokemon_data['HP'])) * 0.7
                self.player1.player_health = 5 * int(self.player1.selected_pokemon_data['HP'])
                self.update_health_bars()
            elif self.player2.selected_pokemon_name == "Pidgeotto":
                self.player2.selected_pokemon_name = "Pidgeot"
                self.player2_name.config(text=f"{self.player2.selected_pokemon_name}")
                self.player2.selected_pokemon_data = self.df[self.df["Name"] == self.player2.selected_pokemon_name].iloc[0]
                self.player2.player_health = (5 * int(self.player2.selected_pokemon_data['HP'])) * 0.7
                self.player1.player_health = 5 * int(self.player1.selected_pokemon_data['HP'])
                self.update_health_bars()
        if winner.score == 3:
            self.plot_health_history()  # health chart is drawn when the winner is determined
            self.plot_damage_history()  # attack chart is drawn when the winner is determined
            self.plot_critical_attacks() # critical attack chart is drawn when the winner is determined
            df_battle = pd.DataFrame(self.battle_data)
            df_battle.to_excel("GameData.xlsx", index=False) #An excel file was created with the information collected throughout the game.
            messagebox.showinfo(title="Game Over!", message=f"{winner.player_name} wins!") #a message box indicating the winner
            self.window.destroy() #for the window to close
        else:
            messagebox.showinfo(title="Game Over!", message=f"{winner.player_name} wins the round!") #a message box indicating the winner

    def physical_attack_player1(self): #A function written to enable player1 to perform physical attacks.
        self.max_attack1_physical = int(self.player1.player_attack)
        damage = random.randint(int(self.player1.player_attack * 0.75), int(self.max_attack1_physical)+1)#The damage value is between seventy-five percent and one hundred percent of the damage value that the pokemon can do.
        if damage: #After the damage is done, the 1st player buttons become deactivated and the 2nd player buttons become active.
            self.button_physical_player1["state"] = tk.DISABLED
            self.button_elemental_player1["state"] = tk.DISABLED
            self.button_physical_player2["state"] = tk.NORMAL
            self.button_elemental_player2["state"] = tk.NORMAL
            self.current_player = self.player2 #The current player becomes the 2nd player.
        self.player2.player_health -= damage #The 2nd player's health is reduced by the damage value.
        self.player1.health_history.append(self.player1.player_health) #Health data is taken for drawing a health chart.
        self.player2.health_history.append(self.player2.player_health) #Health data is taken for drawing a health chart.
        self.player1.attack_history.append(self.player1.player_attack) #Damage values are taken to draw the attack chart.
        self.player2.attack_history.append(self.player2.player_attack) #Damage values are taken to draw the attack chart.
        self.update_health_bars()
        messagebox.showinfo(title=f"{self.player1.player_name} attacks!", message=f"{self.player1.player_name} hit {damage} damage!") #The damage caused by player1 is indicated by a message box.
        self.battle_data.append({
            "Pokemon1": self.player1.selected_pokemon_name,
            "Pokemon2": self.player2.selected_pokemon_name,
            "Health1": self.player1.player_health,
            "Health2": self.player2.player_health,
            "Damage1": damage,
            "Critical1": 0,
            "Element1": 0
        }) #Required information for the excel file was received
        if self.player2.player_health <= 0: #If player 2 loses, the message box is shown.
            messagebox.showinfo(title="Lose!", message=f"{self.player2.player_name} lost! Player1 chooses a pokemon and Player2 gets involved!")
            self.switch_turn()

    def physical_attack_player2(self): #A function written to enable player2 to perform physical attacks.
        self.max_attack2_physical =  int(self.player2.player_attack)
        damage = random.randint(int(self.player2.player_attack * 0.75), int(self.max_attack2_physical)+1) #The damage value is between seventy-five percent and one hundred percent of the damage value that the pokemon can do.
        if damage: #After the damage is done, the 2nd player buttons become deactivated and the 1st player buttons become active.
            self.button_physical_player2["state"] = tk.DISABLED
            self.button_elemental_player2["state"] = tk.DISABLED
            self.button_physical_player1["state"] = tk.NORMAL
            self.button_elemental_player1["state"] = tk.NORMAL
            self.current_player = self.player1 #The current player becomes the 1st player.
        self.player1.player_health -= damage #The 1st player's health is reduced by the damage value.
        self.player1.health_history.append(self.player1.player_health) #Health data is taken for drawing a health chart.
        self.player2.health_history.append(self.player2.player_health) #Health data is taken for drawing a health chart.
        self.player1.attack_history.append(self.player1.player_attack) #Damage values are taken to draw the attack chart.
        self.player2.attack_history.append(self.player2.player_attack) #Damage values are taken to draw the attack chart.
        self.update_health_bars()
        messagebox.showinfo(title=f"{self.player2.player_name} attacks!", message=f"{self.player2.player_name} hit {damage} damage!") #The damage caused by player2 is indicated by a message box.
        self.battle_data.append({
            "Pokemon1": self.player1.selected_pokemon_name,
            "Pokemon2": self.player2.selected_pokemon_name,
            "Health1": self.player1.player_health,
            "Health2": self.player2.player_health,
            "Damage2": damage,
            "Critical2": 0,
            "Element2": 0
        }) #Required information for the excel file was received
        if self.player1.player_health <= 0: #If player 1 loses, the message box is shown.
            messagebox.showinfo(title="Lose!", message=f"{self.player1.player_name} lost! Player2 chooses a pokemon and Player1 gets involved!")
            self.switch_turn()

    def elemental_attack_player1(self): #A function written to enable player1 to perform elemental attacks.
        self.max_attack1_elemental = int(self.player1.player_attack)
        self.Critical1_elemental = 0
        self.double_attack1 = random.randint(80, 101) #Eighty percent chance of doubled damage.
        damage = random.randint(int(self.player1.player_attack * 0.5), int(self.max_attack1_elemental)+1) #The damage value is between fifty percent and one hundred percent of the damage value that the Pokemon can do.
        if damage: #After the damage is done, the 1st player buttons become deactivated and the 2nd player buttons become active.
            self.button_physical_player1["state"] = tk.DISABLED
            self.button_elemental_player1["state"] = tk.DISABLED
            self.button_physical_player2["state"] = tk.NORMAL
            self.button_elemental_player2["state"] = tk.NORMAL
            self.current_player = self.player2 #The current player becomes the 2nd player.
        if damage == self.max_attack1_elemental:
            self.Critical_count['Player 1'] += 1
            messagebox.showinfo(title="Critical!", message="Critical!") #A message box appears stating that the damage is critic damage.
            self.Critical1_elemental += 1 #If the damage is equal to the pokemon's maximum attack, the number of critic attacks increases by 1.
            print(self.player2.player_name,self.Critical2_elemental)
        elif self.player1.player_element == "Fire" and self.player2.player_element == "Grass":
            if 80 < self.double_attack1 < 101:
                damage *= 2 #If player 1's element is stronger than player 2's element, the damage can be doubled.
        elif self.player1.player_element == "Water" and self.player2.player_element == "Fire":
            if 80 < self.double_attack1 < 101:
                damage *= 2 #If player 1's element is stronger than player 2's element, the damage can be doubled.
        elif self.player1.player_element == "Grass" and self.player2.player_element == "Water":
            if 80 < self.double_attack1 < 101:
                damage *= 2 #If player 1's element is stronger than player 2's element, the damage can be doubled.
        elif self.player1.player_element == "Bug" and self.player2.player_element == "Water":
            if 80 < self.double_attack1 < 101:
                damage *= 2 #If player 1's element is stronger than player 2's element, the damage can be doubled.
        if damage == self.max_attack1_elemental: #If the first player's element is stronger than the second player's element, a critical attack may occur.
            if self.player1.player_element == "Fire" and self.player2.player_element == "Grass":#If the first player's element is stronger than the second player's element, a critical attack may occur.
                messagebox.showinfo(title="Critical!", message="Critical!")
                self.Critical1_elemental += 1
                print(self.player1.player_name,self.Critical1_elemental)

            if self.player1.player_element == "Water" and self.player2.player_element == "Fire": #If the first player's element is stronger than the second player's element, a critical attack may occur.
                messagebox.showinfo(title="Critical!", message="Critical!")
                self.Critical1_elemental += 1
                print(self.player1.player_name, self.Critical1_elemental)

            if self.player1.player_element == "Grass" and self.player2.player_element == "Water": #If the first player's element is stronger than the second player's element, a critical attack may occur.
                messagebox.showinfo(title="Critical!", message="Critical!")
                self.Critical1_elemental += 1
                print(self.player1.player_name, self.Critical1_elemental)

            if self.player1.player_element == "Bug" and self.player2.player_element == "Normal": #If the first player's element is stronger than the second player's element, a critical attack may occur.
                messagebox.showinfo(title="Critical!", message="Critical!")
                self.Critical1_elemental += 1
                print(self.player1.player_name, self.Critical1_elemental)

            if self.player1.player_element == "Normal" and self.player2.player_element == "Bug": #If the first player's element is stronger than the second player's element, a critical attack may occur.
                messagebox.showinfo(title="Critical!", message="Critical!")
                self.Critical1_elemental += 1
                print(self.player1.player_name, self.Critical1_elemental)

        self.player2.player_health -= damage #The 2nd player's health is reduced by the damage value.
        self.player1.health_history.append(self.player1.player_health) #Health data is taken for drawing a health chart.
        self.player2.health_history.append(self.player2.player_health) #Health data is taken for drawing a health chart.
        self.player1.attack_history.append(self.player1.player_attack) #Damage values are taken to draw the attack chart.
        self.player2.attack_history.append(self.player2.player_attack) #Damage values are taken to draw the attack chart.
        self.update_health_bars()
        messagebox.showinfo(title=f"{self.player1.player_name} attacks!", message=f"{self.player1.player_name} hit {damage} damage!") #The damage caused by player1 is indicated by a message box.
        self.battle_data.append({
            "Pokemon1": self.player1.selected_pokemon_name,
            "Pokemon2": self.player2.selected_pokemon_name,
            "Health1": self.player1.player_health,
            "Health2": self.player2.player_health,
            "Damage1": damage,
            "Critical1": 1 if self.Critical1_elemental else 0,
            "Element1": 1
        }) #Required information for the excel file was received
        if self.player2.player_health <= 0: #If player 2 loses, the message box is shown.
            messagebox.showinfo(title="Lose!", message=f"{self.player2.player_name} lost! Player1 chooses a pokemon and Player2 gets involved!")
            self.switch_turn()

    def elemental_attack_player2(self): #A function written to enable player2 to perform elemental attacks.
        self.max_attack2_elemental = int(self.player2.player_attack)
        self.Critical2_elemental = 0
        self.double_attack2 = random.randint(80, 101) #Eighty percent chance of doubled damage.
        damage = random.randint(int(self.player2.player_attack * 0.5), int(self.max_attack2_elemental)+1) #The damage value is between fifty percent and one hundred percent of the damage value that the Pokemon can do.
        if damage: #After the damage is done, the 1st player buttons become deactivated and the 2nd player buttons become active.
            self.button_physical_player2["state"] = tk.DISABLED
            self.button_elemental_player2["state"] = tk.DISABLED
            self.button_physical_player1["state"] = tk.NORMAL
            self.button_elemental_player1["state"] = tk.NORMAL
            self.current_player = self.player1 #The current player becomes the 1st player.
        if damage == self.max_attack2_elemental:
            self.Critical_count['Player 2'] += 1
            messagebox.showinfo(title="Critical!", message="Critical!") #A message box appears stating that the damage is critic damage.
            self.Critical2_elemental += 1 #If the damage is equal to the pokemon's maximum attack, the number of critic attacks increases by 1.
            print(self.player2.player_name,self.Critical2_elemental)
        elif self.player2.player_element == "Fire" and self.player1.player_element == "Grass":
            if 80 < self.double_attack2 < 101:
                damage *= 2 #If player 2's element is stronger than player 1's element, the damage can be doubled.
        elif self.player2.player_element == "Water" and self.player1.player_element == "Fire":
            if 80 < self.double_attack2 < 101:
                damage *= 2 #If player 2's element is stronger than player 1's element, the damage can be doubled.
        elif self.player2.player_element == "Grass" and self.player1.player_element == "Water":
            if 80 < self.double_attack2 < 101:
                damage *= 2 #If player 2's element is stronger than player 1's element, the damage can be doubled.
        elif self.player2.player_element == "Bug" and self.player1.player_element == "Water":
            if 80 < self.double_attack2 < 101:
                damage *= 2 #If player 2's element is stronger than player 1's element, the damage can be doubled.

        if damage == self.max_attack2_elemental: #If the second player's element is stronger than the first player's element, a critical attack may occur.
            if self.player2.player_element == "Fire" and self.player1.player_element == "Grass": #If the second player's element is stronger than the first player's element, a critical attack may occur.
                messagebox.showinfo(title="Critical!", message="Critical!")
                self.Critical2_elemental += 1
                print(self.player2.player_name, self.Critical2_elemental)

            if self.player2.player_element == "Water" and self.player1.player_element == "Fire": #If the second player's element is stronger than the first player's element, a critical attack may occur.
                messagebox.showinfo(title="Critical!", message="Critical!")
                self.Critical1_elemental += 1
                print(self.player2.player_name, self.Critical2_elemental)

            if self.player2.player_element == "Grass" and self.player1.player_element == "Water": #If the second player's element is stronger than the first player's element, a critical attack may occur.
                messagebox.showinfo(title="Critical!", message="Critical!")
                self.Critical2_elemental += 1
                print(self.player2.player_name, self.Critical1_elemental)

            if self.player2.player_element == "Bug" and self.player1.player_element == "Normal": #If the second player's element is stronger than the first player's element, a critical attack may occur.
                messagebox.showinfo(title="Critical!", message="Critical!")
                self.Critical2_elemental += 1
                print(self.player2.player_name, self.Critical2_elemental)

            if self.player2.player_element == "Normal" and self.player1.player_element == "Bug": #If the second player's element is stronger than the first player's element, a critical attack may occur.
                messagebox.showinfo(title="Critical!", message="Critical!")
                self.Critical2_elemental += 1
                print(self.player2.player_name, self.Critical2_elemental)

        self.player1.player_health -= damage #The 1st player's health is reduced by the damage value.
        self.player1.health_history.append(self.player1.player_health) #Health data is taken for drawing a health chart.
        self.player2.health_history.append(self.player2.player_health) #Health data is taken for drawing a health chart.
        self.player1.attack_history.append(self.player1.player_attack) #Damage values are taken to draw the attack chart.
        self.player2.attack_history.append(self.player2.player_attack) #Damage values are taken to draw the attack chart.
        self.update_health_bars()
        messagebox.showinfo(title=f"{self.player2.player_name} attacks!", message=f"{self.player2.player_name} hit {damage} damage!") #The damage caused by player2 is indicated by a message box.
        self.battle_data.append({
            "Pokemon1": self.player1.selected_pokemon_name,
            "Pokemon2": self.player2.selected_pokemon_name,
            "Health1": self.player1.player_health,
            "Health2": self.player2.player_health,
            "Damage2": damage,
            "Critical2": 1 if self.Critical2_elemental else 0,
            "Element2": 1
        }) #Required information for the excel file was received
        if self.player1.player_health <= 0: #If player 1 loses, the message box is shown.
            messagebox.showinfo(title="Lose!", message=f"{self.player1.player_name} lost! Player2 chooses a pokemon and Player1 gets involved!")
            self.switch_turn()

    def update_health_bars(self): #It is a function created to edit health labels.
        self.label_health_player1["width"] = (int(5*self.player1.player_health))//10
        self.label_damage_player1["width"] = ((int(5*self.player1.selected_pokemon_data['HP']))//10 - (int(self.player1.player_health))//10)
        self.label3_player1["text"] = f"{self.player1.player_health}/{5*self.player1.selected_pokemon_data['HP']}"
        self.label2_player1["text"] = f"Score: {self.player1.score}"

        self.label_health_player2["width"] = (int(5*self.player2.player_health))//10
        self.label_damage_player2["width"] = ((int(5*self.player2.selected_pokemon_data['HP']))//10 - (int(self.player2.player_health))//10)
        self.label3_player2["text"] = f"{self.player2.player_health}/{5*self.player2.selected_pokemon_data['HP']}"
        self.label2_player2["text"] = f"Score: {self.player2.score}"

    def plot_health_history(self): #The function that creates the health graph
        plt.figure(figsize=(10, 6))
        rounds = range(1, len(self.player1.health_history) + 1)
        plt.plot(rounds, self.player1.health_history, label=self.player1.player_name)
        plt.plot(rounds, self.player2.health_history, label=self.player2.player_name)
        plt.title("Health Analyz")
        plt.xlabel("Turms")
        plt.ylabel("Health")
        plt.legend(loc="upper right")
        plt.show()

    def plot_damage_history(self): #The function that creates the attack graph
        plt.figure(figsize=(10, 6))
        rounds = range(1, len(self.player1.health_history) + 1)
        plt.plot(rounds, self.player1.attack_history, label=self.player1.player_name)
        plt.plot(rounds, self.player2.attack_history, label=self.player2.player_name)
        plt.title("Damage Analyz")
        plt.xlabel("Turms")
        plt.ylabel("Damage")
        plt.legend(loc="upper left")
        plt.show()

    def plot_critical_attacks(self): #Function that creates the graph showing the number of critical attacks
        players = list(self.Critical_count.keys())
        counts = list(self.Critical_count.values())
        plt.bar(players, counts, color=['blue', 'red'])
        plt.xlabel('Players')
        plt.ylabel('Critical Attack Count')
        plt.title('Pokemons')
        plt.show()

def main():
    df = pd.read_csv('pokemon.csv') #pokenmon.csv file read
    df = df.drop(columns=['Type 2', 'Total', 'Defense', 'Sp. Atk', 'Sp. Def', 'Speed', 'Generation', 'Legendary']) #Unwanted columns in pokenmon.csv file have been deleted.
    df = df.rename(columns={'Type 1': 'Element'}) #Changed Type 1 column to Element
    wanted_names = [
        'Bulbasaur', 'Ivysaur', 'Venusaur', 'Charmander', 'Charmeleon',
        'Charizard', 'Squirtle', 'Wartortle', 'Blastoise', 'Caterpie',
        'Metapod', 'Butterfree', 'Weedle', 'Kakuna', 'Beedrill',
        'Pidgey', 'Pidgeotto', 'Pidgeot'
    ] #The name of the desired Pokemon was written in a list.
    df_filtered = df[df['Name'].isin(wanted_names)] #The lines providing the desired names in the pokemon.csv file were retrieved.
    df_filtered.to_csv('cleanPokemons.csv', index=False) #A cleanPokemon.csv file consisting of the desired Pokemon and their data was created.

    pokemon_list = ['Bulbasaur', 'Squirtle', 'Charmander', 'Caterpie', 'Weedle', 'Pidgey'] #A list of pokemon names
    player1_gui = Player("Player 1", pokemon_list) #Makes the relevant class work
    player2_gui = Player("Player 2", pokemon_list) #Makes the relevant class work
    both_players = BothPlayers(player1_gui, player2_gui, df) #Makes the relevant class work

if __name__ == "__main__":
    main()