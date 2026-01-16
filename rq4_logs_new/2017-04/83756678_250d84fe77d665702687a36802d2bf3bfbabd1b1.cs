using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShowInstructions : MonoBehaviour {

	private GameObject instructionFrame;
	private GameObject tutorialFrame;

	void Awake () {
		instructionFrame = GameObject.Find ("Instructions_Holder");
		tutorialFrame = GameObject.Find ("Tutorial_Holder");
	}

	public void ButtonPress() {
		StartCoroutine(ShowTutorial());
	}
	
	private IEnumerator ShowTutorial() {
		tutorialFrame.SetActive (true);
		yield return new WaitForSeconds (3f);
		tutorialFrame.SetActive (false);
		instructionFrame.SetActive (true);
		yield return new WaitForSeconds (5f);
		instructionFrame.SetActive (false);
	}
}