using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ifStatements : MonoBehaviour {

	public int intOne, intTwo, intThree, intFour, intFive, intSix;
	public int value, valueTwo, valueThree;
	public int numOne = 12;
	public int numTwo = 81;
	public Text input;
	public string username = "PlayerOne";
	public string password = "password";
	public string blue = "blue";
	public string tmrw = "tomorrow";
	bool canPlay = true;

	// Use this for initialization
	void Start () {
	// ONE
		print("A + B = C. Fill in these values.");

		if(intOne + intTwo == value) {
			print("The value is correct: " + value); // if the if-statement finds that intOne + intTwo does equal the value entered, it will print the value.
		}else{
			print("That value is incorrect."); // if value is not correct (If statement checks to see if it is) it will print this.
		}

	// TWO
		print("What color is the sky?"); // asks user what the color of the sky is.

		if(input.text == blue){ // if statement checks to see if input text matches "blue". If so it will print this.
			print("I agree that the sky is blue.");
		}else{
			print("That sounds like an interesting color!"); // if the text does not match, it will print this.
		}

	// THREE
		print("What is your username?"); // asks user for username

		if(input.text == username){
			print("Your username is correct."); // Prints when username entered matches the one assigned to Username
		}else{
			print("Your username is incorrect."); // prints if username does not match what is assigned
		}

	// FOUR
		print("What is your password?"); // asks user for password

		if(input.text == password){
			print("Your password is correct."); // prints if password is correct.
		}else{
			print("Your password is incorrect."); //prints if password is incorrect.
		}

	// FIVE
		if(canPlay){ // checks to see if user can play - automatically set to true
			print("You can play! 'canPlay' is set to TRUE."); // prints can play (true) if yes. -- should print this at all times unless otherwise stated
		}else{
			print("Sorry, you can't play. 'canPlay' is set to FALSE."); //prints this if variable changed
		}
	// SIX
		if(intThree - intFour == valueTwo){
			print("ValueTwo is correct!"); // prints this if intThree and intFour equals valueTwo
		}else{
			print("Try again!"); // prints this if valueTwo is incorrect.
		}

	// SEVEN
		if(intFive * intSix == valueThree){
			print("Good job! That's right."); // prints this if int Five and Six multiply to get valueThree.
		}else{
			print("Try again!"); // prints this if it does not match valueThree
		}

	// EIGHT
		print("Complete this sentence... 'The sun will come out ____.'");
		if(input.text == tmrw){
			print("Don't you love that song?"); // prints this if input matches tomorrow
		}else{
			print("Try again...think T..."); // prints this if input does not match tomorrow.
		}
	}

	void Update(){
	// NINE
		if(numOne > 29){
			print("numOne finally hit 29! Stopping here."); // prints this when numOne reaches 29!
		}else{
			numOne++; // adds one to numOne until it hits 29.
		}

	// TEN
		if(numTwo < 50){
			print("numTwo is finally at 50! Did we go back in time?"); // prints this once it minuses one enough times to reach 50.
		}else{
			numTwo--; // subtracts one from numTwo until it hits 50.
		}
	}


} // end of program