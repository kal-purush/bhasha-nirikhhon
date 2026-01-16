using UnityEngine;
using System.Collections;

public class DifficultySelect: MonoBehaviour {

	public GameObject background;
	public GameObject title;
	public GameObject easyButton;
	public GameObject mediumButton;
	public GameObject hardButton;
	public GameObject mainMenuButton;

	public GameObject score;
	public GameObject pizzasLeft;

	public GameObject dontDestroy;

	private DontDestroy information;
	// Use this for initialization
	void Start () {
		information = dontDestroy.GetComponent<DontDestroy>();
	}

	public void selectMode(int mode) {
		background.SetActive(false);
		title.SetActive(false);
		easyButton.SetActive(false);
		mediumButton.SetActive(false);
		hardButton.SetActive(false);
		mainMenuButton.SetActive(false);

		score.SetActive(true);
		pizzasLeft.SetActive(true);
		information.difficulty = mode;
	}
	
	public void returnToMainMenu(){
		Application.LoadLevel(0);
	}
}