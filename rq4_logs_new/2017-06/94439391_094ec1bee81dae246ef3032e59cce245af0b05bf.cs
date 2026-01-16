using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Command : MonoBehaviour {

	public string commandText;

	public Command(string rawCommandText)
	{
		commandText = rawCommandText.Trim ();
	}

	// Use this for initialization
	void Start () 
	{
		
	}
	
	// Update is called once per frame
	void Update () 
	{
		
	}

	public void Run()
	{
		Debug.Log ("Running command \"" + commandText + "\"");
	}

	public string Output(string consoleText, int lineCount)
	{
		Debug.Log ("Printing command \"" + commandText + "\"");
		string prefix = "\n";

		if (consoleText == "") 
		{
			prefix = "";
		} 

		return prefix + "Git-Good:~ cashc$ " + commandText;
	}


}