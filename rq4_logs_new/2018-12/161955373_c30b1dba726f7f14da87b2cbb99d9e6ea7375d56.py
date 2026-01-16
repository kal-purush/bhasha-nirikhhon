#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 15 19:26:25 2018

@author: kart38
"""

avg_lap_time = 0
avg_fuel_used = 0
race_duration = 0
extra_laps = 0
race_laps = 0
fuel_required = 0
# Variable go above here


def user_inquery():
    global avg_lap_time, avg_fuel_used, race_duration, extra_laps
    avg_lap_time = float(input("What is your average lap time?\n"))
    avg_fuel_used = float(input("What is your fuel used per lap?\n"))
    race_duration = float(input("How long is the race in minutes?\n"))
    extra_laps = float(input("How many laps to add for a buffer?\n"))


def fuel_required_func():
    global fuel_required, race_laps, race_duration
    race_length = race_duration * 60
    race_laps = race_length / avg_lap_time
    fuel_required = (race_laps + extra_laps) * avg_fuel_used
    
    
def print_variables():
    print(avg_lap_time)
    print(avg_fuel_used)
    print(race_duration)
    print(extra_laps)
    print(race_laps)
    print(fuel_required)
    
    
def print_results():
    print("\n==== RESULTS ====")
    print(int(race_laps))
    print(int(race_duration))
    print(int(race_laps + extra_laps))
    print(round(fuel_required, 3))
    
    
def main():
    user_inquery()
    fuel_required_func()
    print_variables()
    print_results()
    
main()