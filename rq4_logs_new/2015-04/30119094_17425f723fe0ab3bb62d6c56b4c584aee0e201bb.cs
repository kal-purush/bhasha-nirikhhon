using UnityEngine;
using System.Collections;

public class InstructionNavigation : MonoBehaviour {

	public GameObject pizzaPreparationText;
	public GameObject pstep1Text;
	public GameObject pstep2Text;
	public GameObject pstep3Text;
	public GameObject pstep4Text;

	public GameObject pizzaDeliveryText;
	public GameObject dstep1Text;
	public GameObject dstep2Text;
	public GameObject dstep3Text;

	public GameObject backButton;
	public GameObject nextButton;
	public GameObject startButton;
	public GameObject mainMenuButton;

	private int page;

	// Use this for initialization
	void Start () {
		pizzaPreparationText.SetActive(true);
		pstep1Text.SetActive(true);
		pstep2Text.SetActive(true);
		nextButton.SetActive(true);
		mainMenuButton.SetActive(true);
		page = 0;
	}

	public void next() {
		switch(page) {
			case 0:
				pstep1Text.SetActive(false);
				pstep2Text.SetActive(false);
				pstep3Text.SetActive(true);
				pstep4Text.SetActive(true);
				backButton.SetActive(true);
				break;
			case 1:
				pstep3Text.SetActive(false);
				pstep4Text.SetActive(false);
				pizzaPreparationText.SetActive(false);
				pizzaDeliveryText.SetActive(true);
				dstep1Text.SetActive(true);
				break;
			case 2:
				dstep1Text.SetActive(false);
				dstep2Text.SetActive(true);
				dstep3Text.SetActive(true);
				nextButton.SetActive(false);
				startButton.SetActive(true);
				break;
		}
		page++;
	}

	public void back() {
		switch(page){
			case 0:
				break;
			case 1:
				pstep3Text.SetActive(false);
				pstep4Text.SetActive(false);
				pstep1Text.SetActive(true);
				pstep2Text.SetActive(true);
				backButton.SetActive(false);
				break;
			case 2:
				dstep1Text.SetActive(false);
				pizzaDeliveryText.SetActive(false);
				pizzaPreparationText.SetActive(true);
				pstep3Text.SetActive(true);
				pstep4Text.SetActive(true);
				break;
			case 3:
				startButton.SetActive(false);
				nextButton.SetActive(true);
				dstep2Text.SetActive(false);
				dstep3Text.SetActive(false);
				dstep1Text.SetActive(true);
				break;
		}
		page--;
	}

	public void returnMainMenu() {
		Application.LoadLevel(0);
	}

	public void startGame() {
		Application.LoadLevel(2);
	}
	

}