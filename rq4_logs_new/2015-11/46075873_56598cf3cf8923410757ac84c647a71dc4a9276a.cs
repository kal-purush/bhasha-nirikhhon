using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class RestartGame : MonoBehaviour {

	public Text myText;
	bool isQuitting; 

	void OnStart () {

		isQuitting = false;

	}
	
	void Update () {
	
		if (Input.GetKeyDown (KeyCode.R)) {

			Time.timeScale = 0f;
			isQuitting = true;

		}

		if (isQuitting == true) {

			myText.text = "Do you really want to start over? Your progress will be discarded. [Y]es or [N]o.";
			
			if (Input.GetKeyDown (KeyCode.Y)) {
				Time.timeScale = 1f;
				Application.LoadLevel (Application.loadedLevel);
			} 

			else if (Input.GetKeyDown (KeyCode.N)) {
				Time.timeScale = 1f;
				myText.text = "";
				isQuitting = false;
			}
		}
	}
}