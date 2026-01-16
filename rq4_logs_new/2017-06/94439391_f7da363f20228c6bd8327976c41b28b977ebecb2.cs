using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Step : MonoBehaviour {

	public string answer;
	public string instruction;
	public string correct_response;
	public string error_response;

	public Step (string answer, string instruction, string correct_response, string error_response)
	{
		this.answer = answer;
		this.instruction = instruction;
		this.correct_response = correct_response;
		this.error_response = error_response;
	}

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}
}