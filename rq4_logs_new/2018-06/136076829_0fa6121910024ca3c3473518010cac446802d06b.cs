using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Audio : MonoBehaviour {

	private AudioSource AS; 
	
	private bool play = true; 

	// Use this for initialization
	void Start () {
		AS = GetComponent<AudioSource>(); 
	}
	
	// Update is called once per frame
	void Update () {
		if (play){
			play = false;
			AS.Play(); 
		}
	}
}