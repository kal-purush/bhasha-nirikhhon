using UnityEngine;
using System.Collections;
using UnityEngine.SceneManagement;

public class SessionCompleteCanvasScript : MonoBehaviour {

	bool sessionComplete = false;
	float timeLeft = 3.0f;

	// Use this for initialization
	void Start () {
	
	}  
	
	// Update is called once per frame
	void Update () {
		if (sessionComplete) {
			timeLeft -= Time.deltaTime;
			if (timeLeft <= 0) {
				SceneManager.LoadScene ("UITool");
			}
		}
	}

	public void sessionWillFinish () {
		this.gameObject.SetActive (true);
		sessionComplete = true;
	}
}