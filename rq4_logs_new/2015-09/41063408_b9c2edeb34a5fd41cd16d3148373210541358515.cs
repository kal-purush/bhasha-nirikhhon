using UnityEngine;
using System.Collections;
using UnityEngine.EventSystems;

public class PauseMenu : MonoBehaviour {
	
	bool paused = false;
	bool help = false;
	DropperCamera getClick;
	/// <summary>
	/// Gets the component of the Dropper Camera 
	/// </summary>
	void Start()
	{
		Time.timeScale = 1f;
		getClick = GetComponent<DropperCamera> ();
	}

	/// <summary>
	/// When you press the escape key it pauses the game
	/// </summary>
	void Update()
	{
		//if you press escape it will toggle between on and off
		if (Input.GetKeyDown (KeyCode.Escape)) {
			paused = togglePause ();
		} 
	}
	
	void OnGUI()
	{
		//if paused  = true
		if (paused) {
			GUI.Box ( new Rect (0, 0, Screen.currentResolution.width, Screen.currentResolution.height), "Pause Menu");
			// Make the Quit button.
			if (GUI.Button (new Rect (Screen.currentResolution.width / 2, (Screen.currentResolution.height / 2) + 100, 150, 35), "Quit")) {
				// quits game
				Application.Quit ();
			}

			// Resumes the game when paused
			if (GUI.Button (new Rect (Screen.currentResolution.width / 2, (Screen.currentResolution.height / 2) - 100, 150, 35), "Back")) {
				paused = togglePause();
				
			}

			//Toggles between displaying the keys
			if (GUI.Toggle (new Rect (Screen.currentResolution.width / 2, (Screen.currentResolution.height / 2), 150, 35), help,"Help","Button")!= help) {
				help = !help;
				if (help){
					Debug.Log("Working");
					//GUI.Label(new Rect (200, 100, 150, 35), "Blobbi Moves");
					//GUI.Label(new Rect (200, 150, 150, 35), "Forward:  W");
					//GUI.Label(new Rect (200, 200, 150, 35), "Backward: S");
					//GUI.Label(new Rect (200, 250, 150, 35), "Left:     A");
					//GUI.Label(new Rect (200, 300, 150, 35), "Right:    D");
					//GUI.Label(new Rect (200, 350, 150, 35), "Jump:     Spacebar");

					//GUI.Label(new Rect (Screen.currentResolution.width - 200, 100, 150, 35), "Builders Moves");
					//GUI.Label(new Rect (Screen.currentResolution.width - 200, 150, 150, 35), "Add Block:  		Hold Left Mouse");
					//GUI.Label(new Rect (Screen.currentResolution.width - 200, 300, 150, 35), "Drop Block:			Realease Left Mouse");
					//GUI.Label(new Rect (Screen.currentResolution.width - 200, 200, 150, 35), "Rotate Block:		Right Mouse");
					//GUI.Label(new Rect (Screen.currentResolution.width - 200, 250, 150, 35), "Rotate Screen:  	Mouse Wheel");
					//GUI.Label(new Rect (Screen.currentResolution.width - 200, 350, 150, 35), "Move Screen Up:     Move Mouse Up");
					//GUI.Label(new Rect (Screen.currentResolution.width - 200, 350, 150, 35), "Move Screen Down:   Move Mouse Down");
				}
			}
		}
	}

	/// <summary>
	/// Toggles between a paused playing state.
	/// </summary>
	/// <returns><c>true</c>, if pause was toggled, <c>false</c> otherwise.</returns>
	bool togglePause()
	{
		if(Time.timeScale == 0f)
		{
			// time scale = 1f which unpauses the game
			Time.timeScale = 1f;
			// enables the DropperCamera component so you can add blocks
			getClick.enabled = !getClick.enabled;
			return(false);
		}
		else
		{
			// time scale = 0f which pauses the game.
			Time.timeScale = 0f;
			// disables the DropperCamera component so people can't add blocks while the game is pause
			getClick.enabled = !getClick.enabled;
			return(true);    
		}
	}
}