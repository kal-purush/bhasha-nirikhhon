# -*- coding: utf-8 -*-

import fractions
import random

"""
Glens(nodes):
 [Dynamic]
	-Abundance (integer, resource state)
    -Notes     (string, contains things like position in a regular grid if such a topology exists)
 [Static]
	-Growth    (integer, rate of increase of resource state)
	-Paths	    (unordered list of Path objects, see below)
 [Methods]
   -Increase  (increase the Abundance)
   -Not Yet Linked (checked if the Glen still needs to internally record a Path that links it)
   -Annotate  (change the note on the Glen)
"""

class Glen:

    # Initializer
    def __init__(self, abundance:int=0, growth:int=10, toward_paths=[], away_paths=[], notes:str=""):
        self.abundance = abundance
        self.growth = growth
        self.toward_paths = toward_paths
        self.away_paths = away_paths
        self.notes = notes
        
    
    # Increase abundance of a Glen, either according to its growth rate or by some provided amount
    def increase(self, amount:int=0)->int:
        if amount == 0:
            self.abundance += self.growth
        else:
            self.abundance += amount
            
        return self.abundance
    
    # Is this Glen unregistered as connected from another by some given Path?
    def not_yet_linked_to(self, path)->bool:
        
        return (path not in self.toward_paths)
    
    # Is this Glen unregistered as connected to another by some given Path?
    def not_yet_linked_from(self, path)->bool:
        
        return (path not in self.away_paths)
        
    # Change the note on this Glen
    def annotate(self, note:str)->str:
        if note != "":
            self.notes = note
            
        return self.notes

"""
Paths(edges):
 [Static]
	-Length	(integer, resources consumed to traverse edge)
	-Grade 	(fraction, turns consumed per Length)
	-Glens	(ordered pair of [startingGlen, endingGlen])
	-Symmetrical (boolean)
"""

class Path:
    
    # Initializer
    def __init__(self, to_glen:Glen, from_glen:Glen, symmetrical:bool=False, length:int=1, grade:int=[1,1]):
        self.length = length
        self.grade = fractions.Fraction(grade[0],grade[1])
        self.to_glen = to_glen
        self.from_glen = from_glen
        self.symmetrical = symmetrical

        if to_glen.not_yet_linked_to(self):
            to_glen.toward_paths = to_glen.toward_paths.append(self)
            if symmetrical:
                to_glen.away_paths = to_glen.away_paths.append(self)
                
        if to_glen.not_yet_linked_from(self):
            to_glen.away_paths = to_glen.away_paths.append(self)
            if symmetrical:
                to_glen.toward_paths = to_glen.toward_paths.append(self)
                

"""
Sheep(agents):
 [Dynamic]
	-Prosperity (integer, state based on hunger and endurance)
	-Hunger 	  (integer, number of turns since last meal)
 [Static]
	-Endurance  (integer, degree of hunger required to decrease prosperity)
	-Greed 	  (integer, amount of resources consumed/prosperity gained when fed)
 [Methods]
   -Prosper    (increase Prosperity)
   -Hungrier   (note time since last fed and potentially decrease prosperity)
"""

class Sheep:
    
    # Initializer
    def __init__(self, prosperity:int=0, hunger:int=0, endurance:int=1, greed:int=1):
        self.prosperity = prosperity
        self.hunger = hunger
        self.endurance = endurance
        self.greed = greed
        
        
    # Feed the sheep, 1 unit by default or as specified
    def prosper(self, grass:int=1)->int:
        self.prosperity += grass
        self.hunger = 0
        
        return self.prosperity

    # Note that the sheep has not been fed, for 1 turn by default or a specified number of turns
    def hungrier(self, famine:int=1)->int:
        self.hunger += famine
        if self.hunger >= self.endurance:
            self.prosperity -= 1
            
        return self.hunger
    
"""
Flock(agent collective):
 [Dynamic]
 	-Current Glen		(Glen currently occupied)
	-Laziness	(integer, Length of time so far in current Glen)
	-Members	(unordered list of Sheep objects, see above)
 [Static]
	-Strategy	(Strategy object employed, see below)
	-Notes	(string, description of flock)
 [Methods]
    -Move (move the flock using whatever Stretegy is provided)
"""

class Flock:
    
    # Initializer
    def __init__(self, starting_glen:Glen, strategy, laziness:int=0, members=[], notes:str=""):
        self.current_glen = starting_glen
        self.laziness = laziness
        self.members = members
        self.strategy = strategy
        self.notes = notes
        
    def procreate(lamb:Sheep=Sheep()):
        self.members = self.members.append(lamb)
        return self.members

    # Change the note on this Flock
    def annotate(self, note:str)->str:
        if note != "":
            self.notes = note
        return self.notes
            
    def move(self)->Glen:
        return self.strategy(self)
            

def flee_low_grass(flock:Flock)->Glen:
    glen:Glen = flock.current_glen
    current_grass = glen.abundance
    if current_grass < 2:
        random_selector = random.SystemRandom()
        path:Path = random_selector.choice(glen.away_paths)
        glen = path.to_glen
        
    return glen
        

"""

County(game specification, all static):
    -Glens
    -Flocks
    
	-Maximum Abundance
	-Maximum Growth
	-Maximum Paths
    
	-Maximum Length
	-Maximum Grade
    
	-Maximum Prosperity
	-Maximum Endurance
	-Maximum Greed

Regular County(A county with a regular shape, procedurally generated)
	-Width
	-Height
	-Connectivity
    
HexCounty

GridCounty
    
"""

class County:
    
    # Initializer
    def __init__(self, glens=[], flocks=[]):
        self.glens = glens
        self.flocks = flocks
        
    def init_glen_limits(max_abundance:int=128, max_growth:int=64, max_paths:int=8):
        self.max_abundance = max_abundance
        self.max_growth = max_growth
        self.max_paths = max_paths
        
    def init_path_limits(max_length:int=4, max_grade:int=4):
        self.max_length = max_length
        self.max_grade = max_grade
        
    def init_sheep_limits(max_prosperity:int=8, max_endurance:int=4, max_greed:int=4):
        self.max_prosperity = max_prosperity
        self.max_endurance = max_endurance
        self.max_greed = max_greed