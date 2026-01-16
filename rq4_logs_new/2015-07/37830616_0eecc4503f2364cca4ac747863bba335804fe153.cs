using UnityEngine;
using System.Collections;

public class GameManagerScript : MonoBehaviour {

	[HideInInspector]
	public static GameManagerScript instance = null;

	public BoardManager boardScript;
	public GameObject gameHUD;
	private int sinPoints = 0;

	// Use this for initialization
	void Awake () {
		//Singleton
		if (instance == null)
			instance = this;
		else if (instance != this)
			Destroy (gameObject);

		DontDestroyOnLoad (gameObject);
		boardScript = GetComponent<BoardManager> ();
		InitGame ();
	}

	void InitGame(){
		boardScript.StetupScene ();
		GameObject HudManager = Instantiate (gameHUD);
		HudManager.transform.Find ("NewPunishmentRoomButton").GetComponent<NewPunishmentRoomButton> ().setBoardManager(boardScript);
	}

	public void AddSinPoints(int sinPoints) {	
		this.sinPoints += sinPoints;
		GameHUDScript gameHUDScript = gameHUD.GetComponent<GameHUDScript>();
		gameHUDScript.SetSinPoints (this.sinPoints);
	}

	public void RemoveSinPoints(int sinPoints) {
		AddSinPoints (-sinPoints);
	}
	                                         
}