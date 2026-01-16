using UnityEngine;
using System.Collections;
[ExecuteInEditMode]

public class GUIScript : MonoBehaviour
{
	public Rect setting;
	bool but, wbut;
	bool easy = true, hard, mosthard, inv = false;
	public float val, lval, rval, vval, lvval, rvval;
	string[] buttons = {"Keyboard","Mouse"};
	int press = 0;
	void OnGUI ()
	{
		GUI.Label (new Rect (Screen.width / 2 - 50, Screen.height / 4 - 20, 100, 20), "MAIN MENU");
		if (GUI.Button (new Rect (Screen.width / 2 - 50, Screen.height / 4 + 140, 60, 20), "Exit")) {
			Application.Quit ();
		}
		if (GUI.Button (new Rect (Screen.width / 2 - 50, Screen.height / 4 + 60, 60, 20), "Play")) {
			Application.LoadLevel (1);
		}

		if (GUI.Button (new Rect (Screen.width / 2 - 50, Screen.height / 4 + 100, 60, 20), "Settings")) {
			but = true;
			//	setting = GUI.Window (0, setting, DifficultyLevel, "Level");
		}
		if (but) {
			GUI.Window (0, new Rect ((Screen.width / 3) * 1.8f, Screen.height / 4, 200, 300), SetMenu, "Game Difficulty");
		}
		if (wbut) {
			GUI.Window (1, new Rect (1, 1, Screen.width - 1, Screen.height - 1), SetMenu, "Control Setting");
		}
	}
	void SetMenu (int id)
	{
		if (id == 0) {
			if (easy = GUI.Toggle (new Rect (15, 20, 200, 20), easy, "Easy")) {
				hard = false;
				mosthard = false;
			}
			if (hard = GUI.Toggle (new Rect (15, 40, 80, 20), hard, "Hard")) {
				easy = false;
				mosthard = false;
			}
			if (mosthard = GUI.Toggle (new Rect (15, 60, 80, 20), mosthard, "Most hard")) {
				easy = false;
				hard = false;
			}
			GUI.Label (new Rect (15, 70, 285, 20), "_______________________");
			GUI.Label (new Rect (15, 90, 285, 20), "Game Options");
			GUI.Label (new Rect (15, 110, 50, 20), "Volume");
			val = GUI.HorizontalSlider (new Rect (70, 117, 120, 10), val, lval, rval);
			GUI.Label (new Rect (15, 130, 50, 20), "Video");
			vval = GUI.HorizontalSlider (new Rect (70, 137, 120, 10), vval, lvval, rvval);
			GUI.Label (new Rect (15, 150, 285, 20), "_______________________");
			if (GUI.Button (new Rect (15, 190, 170, 25), "Control Setting")) {
				wbut = true;
			}
			if (GUI.Button (new Rect (15, 240, 170, 25), "Close")) {
				but = false;
			}
		}
		if (id == 1) {
			press = GUI.Toolbar (new Rect ((Screen.width / 2) - 100, Screen.height / 20, 200, 20), press, buttons);
			if (press == 0) {

			}
			if (press == 1) {
				inv = GUI.Toggle (new Rect ((Screen.width / 2) - 100, Screen.height / 10, 200, 20), inv, "Invert Mouse");
			}

		}
		//GUI.Label (new Rect (15, 160, 285, 25), "Control Setting");



	}
	/*void DifficultyLevel (int id)
	{
		if (easy = GUI.Toggle (new Rect (120, 180, 80, 20), easy, "Easy")) {
			hard = false;
			mosthard = false;
		}
		if (hard = GUI.Toggle (new Rect (120, 210, 80, 20), hard, "Hard")) {
			easy = false;
			mosthard = false;
		}
		if (mosthard = GUI.Toggle (new Rect (120, 240, 80, 20), mosthard, "Most hard")) {
			easy = false;
			hard = false;
		}
	}*/
}