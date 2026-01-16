using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Variables : MonoBehaviour {

    /* Variables store information, and are called "variables" because they can change while the program is running.
    When declaring variables, you must specify a name and data type (for instance Integer for whole numbers, Double for decimals,
    and Boolean for true or false, etc.) */

	int example = 1; // establishes an integer called "example" with the value of one.
	int exampleTwo; // you can also establish variables without giving them a value right off the bat...
	const int exampleThree = 10; // establishes a constant variable that cannot be changed when the program is running.

    void Start(){


		exampleTwo = 2; // ...and inputing a value later on.

		example = 5; /*Overrides the previously stated integer value of One.
                       Does not need to be "int example = 5" because it has already been initialized as an integer.*/
	}

        public string firstName = "Jane"; // Ex 4: Using a string variable, you can establish a 'default first name.
        public string lastName = "Doe"; // Ex 5: Using a string variable, you can establish a 'default' last name.
        // Variables as stated previously can allow you to customize these later on with help from user input.


        double pi = 3.14; // Ex 6: A double variable is similar to an integer however it involves numbers with decimals.

        bool a = true; // boolean are for true and false statements.


    }    // end of program