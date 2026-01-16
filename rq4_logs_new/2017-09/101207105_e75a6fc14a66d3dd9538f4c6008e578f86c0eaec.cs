using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class logicalOps : MonoBehaviour {

	bool canPlay = true;

	public int age;

	public int playerLevel;
	public int XP = 0;

	bool levelUp = true;
	bool mount;

	// Use this for initialization
	void Start () {
		
		if(age < 13 && age != 0){ // prints this if the child is younger than teenage years and older than a baby.
			print("You are too young! Sorry. Try playing Minecraft!");
			canPlay = false;
			print("canPlay variable has changed to: " + canPlay);
		}

		if(age > 13 && age < 18){ // will check to see if age is marked greater than 13 and less than 18. If BOTH are true, will do this.
			print("This game is rated M for mature audiences. You cannot play at this time. Grow up just a bit more!");
			canPlay = false;
			print("canPlay variable has changed to: " + canPlay);
		}

		if(age == 18 || age > 18){ // checks to see if age is equal to or greater than 18, if ONE is true it will run.
		// This is saying the same thing as "age >= 18")
			print("You can play! Welcome and enjoy the game.");
		}

		if(playerLevel >= 20 && age >= 18){ // looks at player's age and level to see if they can get to a certain level of the game / play the game in general.
			print("You are high enough level to play!");
			print("Can you play the game? " + canPlay); // will print true because we have not switched it to false.
		}

		if(playerLevel < 20 && playerLevel > 10 && age >= 18){ // looks at playerlevel and age and sees if they can play the boss level
			print("Go XP farming! You need more levels to gain access to this area.");
			print("Can you play the game? " + canPlay); // will print true because we did not change it to be false.
		}

		while(XP <= 100 && playerLevel <= 15){ // once one of these is false, it will stop running.
			print("XP: " + XP + " / Level: " + playerLevel);
			XP += 10; // will add 10 to XP continuously while loop is running
			playerLevel++; // will add one to player level
		}

		if(XP == 100 || playerLevel >= 1){ // if either are true it will print this.
			print("LEVEL UP!"); 
			playerLevel += 1; // adds one to playerLevel
			print("Current level: " + playerLevel);
		}		

		if(!levelUp){ // looks for the opposite or "not" of levelUp (default set to true) so it sees if it is false!
			print("Not quite there yet, keep going!");
		}

		if(levelUp && playerLevel == 10){ // if playerlevel is 10 and levelup is TRUE it will print this
			mount = true; // sets mount bool to true.
			print("You can now ride a mount! " + mount);
		}

		if(playerLevel == 100 || XP == 10000){ // if player level reaches 100 or XP reaches 10,000 (100x100) this will execute
			print("You win!");
			canPlay = false;
			print(canPlay); 
		}	
		
		print("The end.");



	}

}