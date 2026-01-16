using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EndOfLevel : MonoBehaviour {

	public GameObject eolCanvas;
	public string mainMenu;

	// Use this for initialization
	void OnTriggerEnter2D (Collider2D other) {

		Time.timeScale = 0f;
		eolCanvas.SetActive (true);

	}

	public void Quit()
	{
		Application.LoadLevel (mainMenu);
	}

}