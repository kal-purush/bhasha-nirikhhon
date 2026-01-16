using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/*
Name: Alec Reyerson
ID: 1826582
Email: reyer101@mail.chapman.edu
Course: CPSC-340-01
Assignment: Semester Project

Description: This is a script representing an Enemy and its current state
*/

// Enemy
public class Enemy : Fighter {

    public FightController fightController;

    // Awake
    void Awake()
    {

    }

    public string getMove()
    {
        //This is where all fancy logic will go to process what the enemy will do.
        //For now...
        return "nothing";
    }
}

