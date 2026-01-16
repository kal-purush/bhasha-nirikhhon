using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class switchStatements : MonoBehaviour {

    //initializes the planets for the switch cases
    public string[] planet; // creates an array
    // public int i = 0; // indexer
    //Mercury Venus Earth Mars Jupiter Saturn Uranus Neptune

    public void thePlanets(){
        switch(planet){
            case "Mercury"[0]: //case [0]
                print("Mercury is the first planet from the sun.");
                break; // stops case

            case "Venus"[1]: // case [1]
                print("Venus is the second planet from the sun.");
                break; // stops case

            case "Earth"[2]: // case [2]
                print("Earth is the third planet from the sun.");
                break; // stops case

            case "Mars"[3]: // case [3]
                print("Mars is the fourth planet from the sun.");
                break; // stops case

            case "Jupiter"[4]: // case [4]
                print("Jupiter is the fifth planet from the sun.");
                break; // stops case

            case "Saturn"[5]: // case [5]
                print("Saturn is the sixth planet from the sun.");
                break; // stops case
            
            case "Uranus"[6]: // case [6]
                print("Uranus is the seventh planet from the sun.");
                break; // stops case

            case "Neptune"[7]: // case [7]
                print("Neptune is the eighth planet from the sun.");
                break; // stops case

            default: // similar to 'else' clause. All other cases will do this.
                print("There are no other major planets in our solar system.");
                break;

        } // end of switch
    }

}