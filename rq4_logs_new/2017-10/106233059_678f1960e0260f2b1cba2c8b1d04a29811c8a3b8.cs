using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Conditionals : MonoBehaviour {

	int health = -10;
	//bools in C# !
	bool gameOver = false;
	int lives = 3;

	// Use this for initialization
	void Start () {

		//Conditional example
		if (health <= 0) {
			gameOver = true;
		}

		//if+else example:
		if (gameOver == true) {
			Debug.Log ("The game is over!");
		} else {
			Debug.Log ("You're still alive buddy!");
		}
	}
	
	// Update is called once per frame
	void Update () {
		
	}
}