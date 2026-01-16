using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MyAdventure : MonoBehaviour
{
    private enum States
    {
        start,
        intro1,
        intro2,
        delen,
        deelnee,
        deelja,
        leukgesprek
    }

    private States currentState = States.start;

// Start is called before the first frame update
    void Start()
    {
        ShowMainMenu();
    }

    void ShowMainMenu()
    {
        Terminal.ClearScreen();
        Write("Welkom bij HorrorNite");
        Write("Dit is gebasseerd op een waargebeurd verhaal");
        Write("Typ start om te beginnen");
    }

    void StartIntro1()
    {
        currentState = States.intro1;
        Terminal.ClearScreen();
        Write("Het was een koude donkere nacht, en je bent alleen thuis.");
        Write("Dat zorgt voor het ultieme moment om ongestoord Fortnite te doen");
        Write("Typ verder om door te gaan");
    }
    
    void StartIntro2()
    {
        currentState = States.intro2;
        Terminal.ClearScreen();
        Write("Na een paar potjes begin je bekend te raken met een andere speler.");
        Write("Je begint erg gewaagd te raken aan hem, maar vind de uitdaging ook erg leuk.");
        Write("Typ verder om door te gaan");
    }

    void Delen()
    {
        currentState = States.delen;
        Terminal.ClearScreen();
        Write("Deze speler zou graag jou op Discord willen toevoegen, om nog meer potjes samen te kunnen doen");
        Write("Zou jij hem willen toevoegen?");
        Write("Typ ja om gegevens te delen, of nee om deze niet te delen");
    }
    
    void Deelnee()
    {
        currentState = States.deelnee;
        Terminal.ClearScreen();
        Write("Je besloot niet te delen.");
        Write("Toch kreeg je ineens een friendrequest binnen. Je accepteert deze.");
        Write("Typ verder om door te gaan");
    }
    
    void Deelja()
    {
        currentState = States.deelja;
        Terminal.ClearScreen();
        Write("Je besloot te delen.");
        Write("Je krijgt een friendrequest binnen. Je accepteert deze.");
        Write("Typ verder om door te gaan");
    }

    void Write(string tekst)
    {
        Terminal.WriteLine(tekst);
    }

    void OnUserInput(string input)
    {
        switch (currentState)
        {
            case States.start:
                if (input == "start")
                {
                    StartIntro1();
                }else if (input == "1337")
                {
                    Write("Jij bent leet!");
                }
                else
                {
                    Write("Dat commando bestaat niet");
                }
                break;
            case States.intro1:
                if (input == "verder")
                {
                    StartIntro2();
                }else
                {
                    Write("Dat commando bestaat niet");
                }
                break;
            case States.intro2:
                if (input == "verder")
                {
                    Delen();
                }else
                {
                    Write("Dat commando bestaat niet");
                }
                break;
            case States.delen:
                if (input == "ja")
                {
                    Deelja();
                }else if (input == "nee")
                {
                    Deelnee();
                }else
                {
                    Write("Dat commando bestaat niet");
                }
                break;
            default:
                Debug.Log("Er gaat iets fout");
                break;
        }
        /*
        if (currentState == States.start)
        {
            if (input == "start")
            {
                StartIntro1();
            }else if (input == "1337")
            {
                Write("Jij bent leet!");
            }
        }else if (currentState == States.intro1)
        {
            if (input == "verder")
            {
                StartIntro2();
            }
        }else if (currentState == States.intro2)
        {
            if (input == "verder")
            {
                Delen();
            }
        }else if (currentState == States.delen)
        {
            if (input == "ja")
            {
                Deelja();
            }else if (input == "nee")
            {
                Deelnee();
            }
        }
        */
    }
    // Update is called once per frame
    void Update()
    {
        Debug.Log(currentState);
    }

}