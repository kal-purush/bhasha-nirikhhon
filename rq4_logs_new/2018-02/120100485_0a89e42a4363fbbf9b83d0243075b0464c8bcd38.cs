using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Monster : MonoBehaviour {
	public string name;
	public string message;


	public InputField newMessage;
	public Text displayMesssage;


	public Monster[] friends;
	public List<string> feelsLog;
	public Support[] incomingSupport;
	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}

	Monster()
	{
		name = "foo";
		message = "";
	}

	public void setMessage()
	{
		displayMesssage.text = newMessage.text;
		message = displayMesssage.text;
		feelsLog.Add (message);
		for (int i = 0; i < feelsLog.Count; ++i) 
		{
			Debug.Log (feelsLog [i]);
		}
	}

}