using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class mathOperators : MonoBehaviour {
	
	int addOne, addTwo, subOne, subTwo, multOne, multTwo, divOne, divTwo; // initializes some integers
	int addThree, subThree, multThree, divThree; // initializes the sum/difference/products 

	// Use this for initialization
	void Start () {
		
		// Addition example
		addOne = 2; // gives addOne integer a value.
		addTwo = 9; // gives addTwo integer a value.
		
		addThree = addOne + addTwo; // adds addOne and addTwo together, and assigns that value to addThree.
		print("Addition example: " + addThree);

		// Subtraction example
		subOne = 8; // gives int a value
		subTwo = 3; // gives int a value

		subThree = subOne - subTwo; // subtracts subTwo from subOne, assigns difference to subThree.
		print("Subtraction example: " + subThree);

		//Multiplication example
		multOne = 21; // gives int a value
		multTwo = 23; // gives int a value

		multThree = multOne * multTwo; // multiplies multOne to multTwo to get multThree, assigns value to multThree.
		print("Multiplication example " + multThree);

		//Division example
		divOne = 91; // gives int a value
		divTwo = 7; // gives int a value

		divThree = divOne/divTwo; // divides divOne by divTwo and assigns that value to divThree.
		print("Division example: " + divThree);

		//Add one 
		int addFour = 4;
		print("The value of the variable addFour is " + addFour);
		addFour ++; // adds one to the value of addFour.
		print("The value of the variable addFour is " + addFour);
		
		//Subtract one
		int subFour = 9;
		print("The value of the variable subFour is " + subFour);
		subFour --;
		print("The value of the variable subFour is " + subFour);

		//Add and assignment operator
		addFour += addThree; // adds addFour value (now has value of 5 because of add one example) to addThree. 
		// looks like addFour = addFour + addThree, so addFour = 5 + 11 = 16.
		print("Add and assign operation example: " + addFour);

		//Subract and assignment operator
		subFour -= subThree; // subtracts subThree value from subFour value (now has a value of 8).
		// looks like this subFour = subFour - subThree, so subFour = 8 - 5 = 3.
		print("Subtract and assign operation example: " + subFour);

		//Multiply and assignment operator
		int multFour = 11; // assigning value of 11 to a new integer, multFour.
		multFour *= multThree; // multiplies multFour by multThree and assigns that value to multFour.
		// looks like multFour = multFour * multThree, so multFour = 11 * 483 = 5313.
		print("Multiplcation and assign operation example: " + multFour);

		//Divide and assignment operator		
		int divFour = 62; // assigning value of 62 to new integer, divFour.
		divFour /= divThree; // divides divFour by divThree and then assigns that value back to divFour.
		// looks like divFour = divFour / divThree, so divFour = 62 / 13 = 4.769
		// due to the division example having a remainder, it will only print the first digit before the decimal "4"
		print("Division and assign example: " + divFour);

	}
}