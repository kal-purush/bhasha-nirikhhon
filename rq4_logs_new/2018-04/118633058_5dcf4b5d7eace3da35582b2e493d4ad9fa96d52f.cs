using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PauseMenu : MonoBehaviour {

	public string mainMenu;
	//public string inventory;

	public bool isPaused;
	public GameObject pauseMenuCanvas;

	// Update is called once per frame
	void Update () {

		if (isPaused) {
			pauseMenuCanvas.SetActive (true);
			Time.timeScale = 0f;
		} 
		else {
			pauseMenuCanvas.SetActive (false);
			Time.timeScale = 1f;
		}

		if(Input.GetKeyDown(KeyCode.Escape))
			isPaused = !isPaused;

	}

	public void Resume()
	{
		isPaused = false;
	}

	public void Quit()
	{
		Application.LoadLevel (mainMenu);
		//Go to main menu, put this code in once we have a main menu
	}

	public void Inventory() {

		//Go to Inventory management

	}
}