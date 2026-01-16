using UnityEngine;
using System.Collections;

public class Scoreboard : MonoBehaviour {

	public GUIText score;
	public GameObject playAgainButton;
	public GameObject mainMenuButton;

	public GameObject dontDestroy;

	private DontDestroy information;
	// Use this for initialization
	void Start () {
		information = dontDestroy.GetComponent<DontDestroy>();
		score.text = "Score: " + information.totalScore;
	}

	public void playAgain(){
		Application.LoadLevel(2);
	}

	public void mainMenu(){
		Application.LoadLevel(0);
	}
}