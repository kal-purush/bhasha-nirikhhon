using UnityEngine;
using UnityEngine.UI;
using System.Collections;
using System.Collections.Generic;

public class AlgebraQuestion : MonoBehaviour, IQuestion {

	List<GameObject> toggles = new List<GameObject>();
	GameObject correctToggle;

	public int buildQuestion(GameObject pQuestionPanel){
		this.transform.SetParent (pQuestionPanel.transform, false);

		string body = "Look at the ";
		int numStatements = Random.Range (3, 5);
		//Debug.Log ("numStatements=" + numStatements);
		body += numberToString (numStatements);
		body += " statements below:\n\n";
		
		int answerValue = Random.Range (15, 40);
		//Debug.Log ("answerValue=" + answerValue);
		char[] subchars = new char[]{'a','b','c','d'};
		bool smallest = (Random.Range (1, 3) == 1);
		int multiplier = 1;
		if (!smallest) {
			multiplier = -1;
		}
		char answer = subchars [Random.Range (0, numStatements)];
		
		char[] ops = new char[]{'+', '-', 'x', '\u00f7'};
		
		int targetValue;
		char op;
		int x = 0;
		int y = 0;
		for (int i = 0; i < numStatements; i++) {
			x = 0;
			y = 0;
			if (answer == subchars[i]){
				targetValue = answerValue;
			}
			else {
				targetValue = answerValue + (multiplier * Random.Range(1,5));
			}
			//Debug.Log ("targetValue=" + targetValue);
			
			if ((targetValue%2==0) && (ops[3] != ' ')){
				//even
				op = ops[3];
				ops[3] = ' ';
				bool found = false;
				while (!found){
					x = weightedNumber(4);
					if (targetValue%x == 0){
						found = true;
					}
				}
				y = targetValue/x;
			}
			else {
				op = ' ';
				int ind = 0;
				for (int j = 0; j < 30; j++){
					ind = Random.Range(0,4);

					op = ops[ind];
					if (op != ' '){
						break;
					}
				}
				ops[ind] = ' ';
				if (op == ' '){
					op = '+';
					ind = 0;
				}
							
				if (ind == 2){
					// 'x'
					x = weightedNumber(4);
					y = targetValue * x;
				}
				else {
					for (int k = 0; k < 30; k++){
						x = Random.Range(1,10);
						if (ind == 0){
							// +
							y = targetValue + x;
						}
						else if (ind == 3){
							if (targetValue%x == 0){
								y = targetValue/x;
							}
						}
						else {
							// -
							y = targetValue - x;
						}
						if (y > 0){
							break;
						}
					}
					if (y == 0){
						op = '+';
						y = targetValue + x;
					}
				}
			}
			
			body += subchars[i] + " " + op + " " + x + " = " + y + " (" + targetValue + ")\n\n";
		}
		body += "Which letter has the ";
		if (smallest) {
			body += "smallest ";
		} else {
			body += "largest ";
		}
		body += "value? Tick \u2611 a box below to choose\na, b";
		if (numStatements == 3) {
			body += " or c";
		} else {
			body += ", c or d";
		}
		body += ".";
		
		Text text = pQuestionPanel.GetComponentInChildren<Text>();
		text.text = body;
		
		GameObject answerPanel = Instantiate (Prefabs.instance.AnswerPanelPF) as GameObject;
		answerPanel.transform.SetParent (pQuestionPanel.transform, false);
		ToggleGroup toggleGroup = answerPanel.GetComponentInChildren<ToggleGroup> ();
		
		GameObject toggleObject;
		Toggle answerToggle;
		for (int i = 0; i < numStatements; i++) {
			char c = subchars[i];
			toggleObject = Instantiate(Prefabs.instance.AnswerTogglePF) as GameObject;
			toggleObject.transform.SetParent(answerPanel.transform, false);
			answerToggle = toggleObject.GetComponent<Toggle>();
			answerToggle.group = toggleGroup;
			answerToggle.isOn = false;
			toggles.Add(toggleObject);
			if (answer == c){
				correctToggle = toggleObject;
			}
			toggleObject.GetComponentInChildren<Text>().text = ""+c;
		}

		return 1;
	}
	
	static int weightedNumber(int weight){
		int num = 2;
		while (Random.Range (1, weight) != 1) {
			num++;
			if (num == 12){
				num = 2;
			}
		}
		return num;
	}
	
	static string numberToString(int pNumber){
		switch (pNumber) {
		case(0):
			return "zero"; 
		case(1):
			return "one"; 
		case(2):
			return "two"; 
		case(3):
			return "three"; 
		case(4):
			return "four"; 
		case(5):
			return "five"; 
		case(6):
			return "six"; 
		case(7):
			return "seven"; 
		case(8):
			return "eight"; 
		case(9):
			return "nine"; 
		case(10):
			return "ten"; 
		default:
			return "unknown";
		}
	}

	public int markAnswer(){
		Toggle correct = correctToggle.GetComponent<Toggle> ();

		foreach (GameObject toggle in toggles) {
			if (toggle == correctToggle) {
				toggle.GetComponentInChildren<Image> ().color = Color.green;
			} else {
				if (toggle.GetComponent<Toggle>().isOn) {
					toggle.GetComponentInChildren<Image> ().color = Color.red;
				}
			}
			toggle.GetComponent<Toggle>().interactable = false;
		}

		int marks = 0;
		if (correct.isOn) {
			marks++;
		}

		return marks;
	}
}