using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ToggleGameObject : MonoBehaviour {

	public bool showGameObject = false;

	// Use this for initialization
	void Start () {
		gameObject.SetActive (false);
	}
	
	// Update is called once per frame
	void Update () {
		
	}

	public void Toggle()
	{
		if (showGameObject == false) {
			gameObject.SetActive (true);
			showGameObject = true;
		} else {
			gameObject.SetActive (false);
			showGameObject = false;
		}
	}
}