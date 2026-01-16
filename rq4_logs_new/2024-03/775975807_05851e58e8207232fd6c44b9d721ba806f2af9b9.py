#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CarPark Project
Coursework for: 4009B Programming for applications


@author: 123456789, 100363107
@date:   19/03/2024

"""

class Car:
    def __init__(self, model, colour, reg):
        """
        A constructor method/initaliser that recieves the model. colour and reg plate as 
        from the class arguments when its called. 
        Args:
            model (string): The Model of the Car. A string.
            colour (string): The colour of the Car. A string.
            reg (string): The registration letters and numbers string.
        """
        self.model = model
        self.colour = colour
        self.reg = reg
    
    def __str__(self):
        """
        The string method returns the constructor variables. It turns them into a string
        displaying the model, colour and registration in a row.
        
        Returns:
            string: f string with class variable model, colour and reg.
        """
        return f"Your car: {self.model}, {self.colour}, {self.reg}"
    

def tester():
    """The testing function - not accessed as part of this file as a library. 
    """
    carList = []
    car1 = Car("Toyota", "Green", "BD5I SMR")
    car2 = Car("Subaru", "Blue", "GP3D 9KX")
    car3 = Car("Nissan", "Black", "TL4P 4ZL")
    carList.append(car1)
    carList.append(car2)
    carList.append(car3)
    for i in carList:
        print(i)
    
if __name__ == "__main__":
    tester()
    
    