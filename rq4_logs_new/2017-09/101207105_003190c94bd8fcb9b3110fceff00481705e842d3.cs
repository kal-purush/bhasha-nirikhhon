using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class whileLoops : MonoBehaviour {

	public int a = 3;
	public int b = 6;
	public int c = 9; 

	public int d = 10;
	public int e = 12;

	public int f = 15;

	public int health = 100;

	public bool play = true;

	public string hello = "Hello";
	public Text input; 


	// Use this for initialization
	void Start () {
		StartCoroutine(allTheloops()); // calls the IEnumerator.
	}
	IEnumerator allTheloops(){
		while(a > 1 && a < 5){ // does this while a is greater than one and less than 5.
			print(a);
			a++; // adds one to a, making it loop until it hits 5.
		}

		while(b <= 10){
			print(b); // prints b.
			b++; //adds one to b, eventually also ending this loop.
		}


		while(c <= 9){ // performs while loop while 9 is less than or equal to 9.
			print("Good math skills.");
			c++; // adds one to C value, stopping the while loop.
		}

		 while(input.text == hello){ // gets input and prints this if true
		 	print("hello to you too!");
		 	yield return new WaitForSeconds(.5f); // stops the while loop after .5 seconds, or it would continously loop when true.
		}

		while(e < 50){
			print(e);
			e++; // adds one to E until it reaches 50, then loop stops.
		}

		while(f > 10){
			print(f);
			f--; // minuses one from F until it hits 10, ending loop.
			}
		
		while(d < 20){
			print(d);
			d++; // adds one to D until it hits 20, ending the loop.
		}

		while(health < 100 && health > 0){
			print("you are losing health! Find a health pack!");
			health--; // Minuses one from health until it hits zero and the loop stops.
		}

		if(health == 0){
			print("You are dead!"); // prints this once health equals zero.
		}

		while(play){
			print("You can play!"); // prints this because play is set to TRUE
			play = false; // sets play to false, ending loop
		}
			print("Sorry, you can no longer play."); // prints this because play is now false.
			

	/*	while(play==false){
			print("would you like to play?"); // prints this when play is false.
		} THIS WILL CREATE AN INFINITE LOOP, AND NOT MOVE ON. */




	}
}