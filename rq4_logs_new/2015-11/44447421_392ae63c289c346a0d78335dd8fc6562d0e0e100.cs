using UnityEngine;
using System.Collections;

public class GameControl : MonoBehaviour {

	//public variables
	public Light playerLight;
	public Light mainLight;
	public GameObject platform;

	//private variables

	//=============================================================================================================
	//Unity provided functions
	// Use this for initialization
	void Start () {
		platform.SetActive (false);
	}
	
	// Update is called once per frame
	void Update () {
		if (Input.GetKeyDown (KeyCode.L))
			turnOn ();
	}

	//============================================================================================================
	//custom functions
	public void turnOn()
	{
		playerLight.enabled = false;
		mainLight.intensity = 1;
		platform.SetActive (true);
	}
}