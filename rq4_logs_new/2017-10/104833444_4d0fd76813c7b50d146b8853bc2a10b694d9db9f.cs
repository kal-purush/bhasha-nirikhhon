using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/*
Name: Alec Reyerson
ID: 1826582
Email: reyer101@mail.chapman.edu
Course: CPSC-340-01
Assignment: Semester Project

Description: This is a class representing a move to be used in the Fight scene by a Fighter
*/

// Move
public class Move {
    public int damage, staminaCost, turns;
    public string name;

    /*
   Name: Move
   Parameters: string _name, int _damage, int _staminaCost, int _turns
   */
    public Move(string _name, int _damage, int _staminaCost, int _turns)
    {
        this.name = _name;
        this.damage = _damage;
        this.staminaCost = _staminaCost;
        this.turns = _turns;
    }	
}