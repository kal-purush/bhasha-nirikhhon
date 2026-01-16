using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class PauseScript : MonoBehaviour {

	//public variables
	public string startScreen;
	public bool paused = false;
	public GameObject pauseMenu_canvas;
	public GameObject cInfo;
	public GameObject cInfo_background;

	//private variables
	bool showControls = false;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		//pause and unpause the game
		if (paused) {
			pauseMenu_canvas.SetActive (true);
			Time.timeScale = 0f;
		}
		else {
			pauseMenu_canvas.SetActive(false);
			Time.timeScale = 1f;
		}

		if (Input.GetKeyDown (KeyCode.Escape)) {
			paused = !paused;
		}

		//show/don't show the control info box
		if (showControls) {
			cInfo.SetActive(true);
			cInfo_background.SetActive(true);
		}
		else
		{
			cInfo.SetActive(false);
			cInfo_background.SetActive(false);
		}
	}

	public void ResumeLevel()
	{
		paused = false;
	}

	public void Quit()
	{
		//Application.Quit ();
	}

	public void ToStart()
	{
		//Application.LoadLevel(startScreen);
	}

	public void ControlInfo()
	{
		showControls = !showControls;
	}
}