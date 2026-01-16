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

public class StandardAttack : Move {

	public StandardAttack(string _name) : base(_name) {}

    new public Dictionary<string, int> processMove(Fighter attacker, Fighter target)
    {
        switch(attacker.weapon)
        {
            case "Spoon":
                return new Dictionary<string, int>()
                {
                    {Constants.HP, -1 }
                };                
            case "Rolling Pin":
                return new Dictionary<string, int>()
                {
                    {Constants.HP, -2 }
                };
            case "Frying Pan":
                return new Dictionary<string, int>()
                {
                    {Constants.HP, -4 }
                };
        }
        return null;
    }
}