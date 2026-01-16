using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using UnityEngine;
using UnityEngine.UI;

public class DialogueController : MonoBehaviour {

	private Text dialogueText;

	public List<List<string>> dialogueList = new List<List<string>>();
	private List<string> tempList = new List<string>();
	private List<string> splitLine = new List<string> ();

	private string currentDialogue = "";
	private int currentCharID;
	private int lineNo = 0;
	private bool isDialogue = false;

	void Awake() {
		dialogueText = (GameObject.Find("Dialogue")).GetComponent<Text>();
		dialogueText.text = "";
	}

	void Update () {
		if (Input.GetButtonDown("Fire1") && isDialogue) {
			ChangeDialogue();
		}
	}
	
	private void Importer(string filename) {
		StreamReader selectedFile = new StreamReader (Application.dataPath + "/Dialogue/" + filename + ".txt");

		tempList = selectedFile.ReadToEnd ().Split (new string[] { "\r\n" }, System.StringSplitOptions.RemoveEmptyEntries).ToList();
		foreach(string i in tempList) {
			splitLine = i.Split(new string[] { "|" }, System.StringSplitOptions.RemoveEmptyEntries).ToList();
			dialogueList.Add(splitLine);
		}
		selectedFile.Close ();
		return;
	}

	private void ChangeDialogue() {
		if (lineNo < dialogueList.Count) {
			currentDialogue = (string)dialogueList [lineNo] [1];
			currentCharID = System.Int32.Parse ((string)dialogueList [lineNo] [0]);

			dialogueText.text = currentDialogue;
			lineNo++;
		} else {
			EndDialogue();
		}
	}

	public void StartDialogue(string filename) {
		Importer(filename);
		isDialogue = true;
		Time.timeScale = 0.0f;
		lineNo = 0;
		ChangeDialogue();
	}

	private void EndDialogue() {
		dialogueText.text = "";
		isDialogue = false;
		Time.timeScale = 1.0f;
	}
}