using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Entry : MonoBehaviour {

	public string entryText;

	public Entry(string rawentryText)
	{
		entryText = rawentryText.Trim ();
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
		Debug.Log ("Running entry \"" + entryText + "\"");
	}

	public string Output(string consoleText, int lineCount)
	{
		Debug.Log ("Printing entry \"" + entryText + "\"");
		string prefix = "\n";

		if (consoleText == "") 
		{
			prefix = "";
		} 

		return prefix + "Git-Good:~ cashc$ " + entryText;
	}

}