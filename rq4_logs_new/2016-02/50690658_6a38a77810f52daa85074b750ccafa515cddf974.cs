using UnityEngine;
using System.Collections;

public class GlobalInputs : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {

		if(Input.GetKeyDown(KeyCode.Escape)){
			print ("quit");
			Application.Quit();
		}
	}
}