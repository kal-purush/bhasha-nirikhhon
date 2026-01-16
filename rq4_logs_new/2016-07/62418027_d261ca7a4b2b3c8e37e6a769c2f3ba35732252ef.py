"""Module that holds Scene classes for use in the text adventure game.
    
    Classes:
        Scene - The base class for each scene in the game
        Death - Scene for when player dies
        Finished - Scene for when game is completed
        Central_Corridor - Scene we typically start in
        Armory - Scene in the laser weapon armory
        Escape_Pod - Scene where player escapes
        Bridge -  Scene where bridge is taken
    """

#-- Import Statements --#
import sys
from random import randint

#-- Class Definitions --#
class Scene(object) :
    """A base class for each scene in the game."""

    # Prevent creation of instance objects of type Scene
    def __init__(self) :
        raise NotImplementedError

    # Abstract method that will be implemented in all subclasses
    def enter(self) :
        """Not implemented. Subclass and provide functionality.

        Returns:
            the name of the next scene in the players path 
        """
        pass

    def __eq__(self, other) :
        # Just check if type of Scene are the same.
        return type(self) == type(other)


class Death(Scene) :
    """---TODO---"""
    
    def __init__(self) :
        pass

    def enter(self) :
        pass

class Finished(Scene) :
    """---TODO---"""

    def __init__(self) :
        pass

    def enter(self) :
        pass

class Central_Corridor(Scene) :
    """---TODO---"""
    
    def __init__(self) :
        pass

    def enter(self) :
        pass
    

class Armory(Scene) :
    """---TODO---"""
    
    def __init__(self) :
        pass

    def enter(self) :
        pass
    

class Escape_Pod(Scene) :
    """---TODO---"""
    
    def __init__(self) :
        pass

    def enter(self) :
        pass
    

class Bridge(Scene) :
    """---TODO---"""
    
    def __init__(self) :
        pass
    
    def enter(self) :
        pass