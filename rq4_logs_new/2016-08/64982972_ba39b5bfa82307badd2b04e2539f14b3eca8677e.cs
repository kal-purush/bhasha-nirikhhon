using UnityEngine;
using System.Collections;

public class MorseCode : MonoBehaviour {

	public prt_PlayerController player;

	public float dotDashTiming = 0.5f; //how long to hold down to make a dash
	public float delayMCode = .75f; // how

	string command;
	float timeHeld;
	float lastPressed;

	void Update(){
		float initialPress = 0.0f;

		if (Input.GetKeyDown("space")){
			initialPress= Time.time;
			Debug.Log("Space bar down");

		}
		if (Input.GetKeyUp("space")){
			lastPressed = Time.time;
			timeHeld = Time.time - initialPress;
			Debug.Log("Space bar up");
		}

		float timeSinceLastPress = Time.time - lastPressed;
		string morseCode = "";

		if(timeSinceLastPress <= 0); //do nothing 
		else if(timeSinceLastPress < .75f)
			morseCode += dotOrDash(timeHeld);
		else if (timeSinceLastPress < 1.75f) 
			command += getCharacter();
		else if (timeSinceLastPress < 1.5f){
			command += " ";
			Debug.Log("Entered space");
		} 
		else
		{
			getCommand(command);
			resetTimers();
		}
	}

	string dotOrDash(float timeHeld){
		string character = "";
		if (timeHeld < dotDashTiming){
			character = ".";
			Debug.Log("Entered dot");
		}
		else {
			character = "-";
			Debug.Log("Entered dash");
		}

		lastPressed = 0.0f;
		return character;
	}

	void getCommand(string command){
		if (command == "go a1 ")
			player.goToOrigin();
		else
			Debug.Log("didn't enter correct command " + command);

		
	}

	void resetTimers(){
		timeHeld = 0;
		lastPressed = 0;
	}

	string getCharacter(string dotsAndDashes){
		string character = "";
		switch (dotsAndDashes)
		{
			case ".-":
					character = "a"; //alphabet
					break;
			case "-...":
					character = "b";
					break;
			case "-.-.":
					character = "c";
					break;
			case "-..":
					character = "d";
					break;
			case ".":
					character = "e";
					break;
			case "..-.":
					character = "f";
					break;
			case "--.":
					character = "g";
					break;
			case "....":
					character = "h";
					break;
			case "..":
					character = "i";
					break;
			case ".---":
					character = "j";
					break;
			case "-.-":
					character = "k";
					break;
			case ".-..":
					character = "l";
					break;
			case "--":
					character = "m";
					break;
			case "-.":
					character = "n";
					break;
			case "---":
					character = "o";
					break;
			case ".--.":
					character = "p";
					break;
			case "--.-":
					character = "q";
					break;
			case ".-.":
					character = "r";
					break;
			case "...":
					character = "s";
					break;
			case "-":
					character = "t";
					break;
			case "..-":
					character = "u";
					break;
			case "...-":
					character = "v";
					break;
			case ".--":
					character = "w";
					break;
			case "-..-":
					character = "x";
					break;
			case "-.--":
					character = "y";
					break;
			case "--..":
					character = "z";
					break;
			case ".----":
					character = "1"; //numbers
					break;
			case "..---":
					character = "2";
					break;
			case "...--":
					character = "3";
					break;
			case "....-":
					character = "4";
					break;
			case ".....":
					character = "5";
					break;
			case "-....":
					character = "6";
					break;
			case "--...":
					character = "7";
					break;
			case "---..":
					character = "8";
					break;
			case "----.":
					character = "9";
					break;
			case "-----":
					character = "0";
					break;
			default:
				character = "?";
				break;
		}

		Debug.Log("entered " + character);
		return character;
	}

}