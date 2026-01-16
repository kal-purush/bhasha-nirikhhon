using UnityEngine;
using System.Collections;

public class TitleScreenScript : MonoBehaviour {

	//public variables
	public GameObject infoMenuCanvas;
	public string firstLevel;

	//private variables
	bool bringUpInfo = false;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {

		if (Input.GetKeyDown (KeyCode.Escape)) {
			bringUpInfo = false;
		}

		if (bringUpInfo) {
			infoMenuCanvas.SetActive (true);
		}
		else {
			infoMenuCanvas.SetActive(false);
		}
	}

	public void TurnOnInfo()
	{
		bringUpInfo = true;
	}

	public void StartGame()
	{
		Application.LoadLevel (firstLevel);
	}

	public void QuitGame()
	{
		Application.Quit ();
	}
}