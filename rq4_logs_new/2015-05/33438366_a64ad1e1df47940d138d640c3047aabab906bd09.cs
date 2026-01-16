using UnityEngine;
using System.Collections;

public class PauseScreen : MonoBehaviour {
	
	public bool paused = false;
	private RectTransform panel;
	private Canvas canvas;

	void Awake() {
		//panel = GetComponent <RectTransform> ();
		canvas = GameObject.Find("Canvas").GetComponent<Canvas>();
	}

	// Use this for initialization
	void Start () {
		canvas.enabled = false;
	}
	
	// Update is called once per frame
	void Update () {
		if (Input.GetKeyDown (KeyCode.P)) {
			if(paused) {
				paused = false;
				canvas.enabled = false;
				Time.timeScale = 1;
			} else {
				paused = true;
				canvas.enabled = true;
				Time.timeScale = 0;
			}
		}
	}

	public void ResumeGame () {
		paused = false;
		canvas.enabled = false;
		Time.timeScale = 1;
	}
}