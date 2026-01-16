using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Hacker : MonoBehaviour {

	// Use this for initialization
	void Start () 
    {
        MainMenu();
	}

    void MainMenu()

    {
        Terminal.ClearScreen();
        string welcome = "Hello Anonymous,";
        Terminal.WriteLine(welcome);
        Terminal.WriteLine("There is only one way,hack and escape or get caught");
        Terminal.WriteLine("Are you brave enough to continue ?");
        Terminal.WriteLine("\nPress 1 for the Bank ");
        Terminal.WriteLine("Press 2 for the Airport ");
        Terminal.WriteLine("Press 3 for the SpaceX ");
        Terminal.WriteLine("Enter your choice: ");  
    }
	
	// Update is called once per frame
	void Update () {
		
	}
}