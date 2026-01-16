
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class gameOver : MonoBehaviour {
	public GameObject text;
	public GameObject reset;
	// Use this for initialization
	void Start () {
		PlayerPrefs.SetInt ("isGameOver", 0);
		text.gameObject.SetActive (false);
		reset.gameObject.SetActive (false);
	}

	// Update is called once per frame
	void Update () {
		if (PlayerPrefs.GetInt ("isGameOver") == 1) {
			reset.gameObject.SetActive (true);
			text.gameObject.SetActive (true);
		}


	}
}