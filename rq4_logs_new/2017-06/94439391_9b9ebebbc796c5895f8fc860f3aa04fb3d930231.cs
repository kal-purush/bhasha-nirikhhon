using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class InputFieldManager : MonoBehaviour {

	public Text consoleTextBox;
	public InputField commandLine;

	// Use this for initialization
	void Start () {
		commandLine.text = "";
	}
	
	// Update is called once per frame
	void Update () {
		commandLine.ActivateInputField();
	}

	// Sends a string to the console
	public void SendToConsole() {
		if (Input.GetButtonDown ("Submit")) {
			print ("SendToConsole: " + commandLine.text);
			string currText = consoleTextBox.text;
			consoleTextBox.text = currText + "\n" + commandLine.text;
			commandLine.text = "";
		}

	}
}