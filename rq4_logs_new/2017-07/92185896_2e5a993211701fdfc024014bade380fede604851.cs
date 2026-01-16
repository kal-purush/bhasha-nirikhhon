using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameController : MonoBehaviour {
	public bool timeElapsed = false;
	public bool gameWon = false;
	
	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
		if (gameWon)
		 {
			 Application.LoadLevel("EndStateWin");
		 }
	 
		 if (timeElapsed)
		 {
			 Application.LoadLevel("EndStateLose");
		 }
	}
	
	
	
	 //If the game controller receives this signal from the timer, it will end the game
	 void timeHasElapsed()
	 {
		 timeElapsed = true;
	 }
}