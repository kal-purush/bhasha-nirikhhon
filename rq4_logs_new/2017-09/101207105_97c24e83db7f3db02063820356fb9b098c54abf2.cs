using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class planetSwitch : MonoBehaviour {

    //initializes the planets for the switch cases
   public string[] planet; // creates an array
   public int i = 0; // indexer
	public int planets = 1; // initializes integer

	public void Start(){
		while(planets <= 8){ // while number of planets is less than or equal to 8 it will do this
			print("There are now " + planets + " planets in our solar system.");
			planets++;// adds one to planets and goes again
		}

		thePlanets(); // runs thePlanets 
	}

	public void thePlanets(){
        switch(planet[i]){
            case "Mercury": //case [0]
                print("Mercury is the first planet from the sun.");
                break; // stops case

            case "Venus": // case [1]
                print("Venus is the second planet from the sun.");
                break; // stops case

            case "Earth": // case [2]
                print("Earth is the third planet from the sun.");
                break; // stops case

            case "Mars": // case [3]
                print("Mars is the fourth planet from the sun.");
                break; // stops case

            case "Jupiter": // case [4]
                print("Jupiter is the fifth planet from the sun.");
                break; // stops case

            case "Saturn": // case [5]
                print("Saturn is the sixth planet from the sun.");
                break; // stops case
            
            case "Uranus": // case [6]
                print("Uranus is the seventh planet from the sun.");
                break; // stops case

            case "Neptune": // case [7]
                print("Neptune is the eighth planet from the sun.");
                break; // stops case

			case "Pluto": // case [8]
				print("Sorry! Pluto is no longer classified as a planet.");
				break; // stops the case

            default: // similar to 'else' clause. All other case numbers called that don't fall in between 0-8 will do this.
                print("There are no other major planets in our solar system.");
                break;

         } // end of switch	

    } // end of thePlanets

}