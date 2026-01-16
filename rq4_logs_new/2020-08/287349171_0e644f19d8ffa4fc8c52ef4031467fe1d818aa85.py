class Pokemon:

    def __init__(self,name, level, type_1 ):

        self.name = name
        self.level = level
        self.type_1 = type_1
        self.maximum_health = level * 10
        self.current_health = self.maximum_health
        self.knock_out = False
        self.experience_points = 0

    def lose_health(self, points):

        if points > self.current_health:
            self.current_health = 0
            self.knock_out = True
            print("Pokemon has been knocked out")
        
        else:
            self.current_health -= points
            print("Pokemon now has {} health points!".format(self.current_health))

    
    def gain_health(self, points):

        if self.maximum_health >= (self.current_health + points):
            self.current_health = self.maximum_health
            print("{} now has {} health points!".format(self.name, self.current_health))
            return
        else:
            print("{} has {} health points, which is equal to maximum health!".format(self.name, self.current_health))
            self.current_health += points
            return

    def revive(self):

        if self.knock_out == True:
            self.current_health = self.maximum_health
            self.knock_out = False
            print("{} has been revived".format(self.name))
            return
        else:
            print("{} is not knocked out. It can't be revived!".format(self.name))
            return

    def attack(self, pokemon):

            if self.knock_out == False and pokemon.knock_out == False:
                if self.type_1 != pokemon.type_1:
                    if (self.type_1 == "Grass" or self.type_1  == "Fire") and (self.type_1  == "Grass" or self.type_1 == "Fire"):
                        if self.type_1 == "Fire":
                            winner = self.name
                            loser = pokemon.name
                            print("{} has beaten {}".format(winner, loser))
                            loser.lose_health(self.level * 2)
                            return
                        else:
                            winner = pokemon.name
                            loser = self.name
                            print("{} has beaten {}".format(winner, loser))
                            pokemon.lose_health(pokemon.level * (1/2))
                        return
            

                    if (self.type_1 == "Grass" or self.type_1  == "Water") and (self.type_1  == "Grass" or self.type_1 == "Water"):
                        if self.type_1 == "Grass":
                            winner = self.name
                            loser = pokemon.name
                            print("{} has beaten {}".format(winner, loser))
                            loser.lose_health(self.level * 2)
                            return
                        else:
                            winner = pokemon.name
                            loser = self.name
                            print("{} has beaten {}".format(winner, loser))
                            loser.lose_health(pokemon.level * (1/2))
                            return
                
                    if (self.type_1 == "Fire" or self.type_1  == "Water") and (self.type_1  == "Fire" or self.type_1 == "Water"):
                        if self.type_1 == "Water":
                            winner = self.name
                            loser = pokemon.name
                            print("{} has beaten {}".format(winner, loser))
                            loser.lose_health(self.level * 2)
                            return
                        else:
                            winner = pokemon.name
                            loser = self.name
                            print("{} has beaten {}".format(winner, loser))
                            loser.lose_health(pokemon.level * (1/2))
                            return
        
                else:
                    if self.level >= pokemon.level:
                        print("{} has beaten {}".format(self.name, pokemon.name))
                        pokemon.lose_health(self.level)
                        return
                    else:
                        print("{} has beaten {}".format(pokemon.name, self.name))
                        self.lose_health(pokemon.level)
                        return
            else:
                print("You can't battle with knocked out pokemon")

    def level_up(self):
        if self.experience_points >= 10:
            self.level += 1
            self.experience_points -= 10
            print("Your {} is now level {}".format(self.name, self.level))
        

class Trainer:

    def __init__(self, name):

        self.name_trainer = name
        self.num_pokemons = 0
        self.currently_active = None
        self.num_of_potions = 5
        self.max_num = 6
        self.pokemons = []


    def currently_active_method(self, name):
        for i in range(len(self.pokemons)):
            if name  == self.pokemons[i]:
                if name.knock_out == False:
                    self.currently_active = i
                    print("{} was set as current pokemon.".format(name.name))
                    return
                else:
                    print("This pokemon is knocked out")
                    return
            
        

    def use_potion(self):

        if self.num_of_potions > 0:

            if self.currently_active != None :
                if self.pokemons[self.currently_active].current_health > 0:
                    pokemon = self.pokemons[self.currently_active]
                    pokemon.gain_health(pokemon.level * 3)
                    self.num_of_potions -= 1
                    print("Now you have {} potions".format(self.num_of_potions))
                else:
                    pokemon = self.pokemons[self.currently_active]
                    pokemon.revive(pokemon.level * 3)
                    self.num_of_potions -= 2
                    print("Now you have {} potions".format(self.num_of_potions))     
    
    def battle(self, trainer):
        if self.currently_active != None and trainer.currently_active != None:
            pokemon = self.pokemons[self.currently_active]
            pokemon_1 = trainer.pokemons[trainer.currently_active]
            print("Battle has started between you and  {}".format(trainer.name_trainer))
            pokemon.attack(pokemon_1)
            pokemon.experience_points += 10
            pokemon_1.experience_points += 10
            pokemon.level_up()
            pokemon_1.level_up()
            return
    
    def add_pokemon(self, pokemon):
        if isinstance(pokemon, Pokemon):   
            self.pokemons.append(pokemon)
            self.num_pokemons += 1
            print("Added new pokemon")

  
        
       

          
bulbasar = Pokemon("Bulbasar", 3, "Water")
pikachu = Pokemon("Pikachu", 5, "Grass")


miha = Trainer("Miha")
floyd = Trainer("Floyd")
miha.add_pokemon(pikachu)
floyd.add_pokemon(pikachu)
miha.currently_active_method(pikachu)
floyd.currently_active_method(pikachu)
floyd.battle(miha)


    