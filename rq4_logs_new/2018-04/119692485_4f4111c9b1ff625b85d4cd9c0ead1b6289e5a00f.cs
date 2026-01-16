using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class changeTextOnClick : MonoBehaviour {

	// Use this for initialization
	void Start () {

	}

	// Update is called once per frame
	void Update () {

	}

	// this happens everytime the buttion is clicked
	public void ButtonOnClick() {
		if(GameObject.Find("msg_I_2").GetComponent<Text>().enabled == true){
			// SceneManager.LoadScene(scene);
			Debug.Log("Should now close this popup and display overworld");
		}
		else {
			GameObject.Find("msg_I_1").GetComponent<Text>().enabled = false;
			GameObject.Find("msg_I_2").GetComponent<Text>().enabled = true;
		}
	}

}