using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using UnityEngine.UI;

using System.Collections.Generic;

public class Addition : MonoBehaviour {
	public Text firstNum;
	public Text secondNum;
	int counter = 0;

	public Button button1;
	public Button button2;
	public Button button3;
	public Button button4;

	int firstNumMin = 5;
	int firstNumMax = 20;
	int seconfNumMin = 10;
	int secondNumMax = 25;

	public int answer;
	void Start () 
	{
	}
	

	void Update () 
	{
		counter++;
		if (counter == 1) {
			AddingNums ();
		}
	}

	public  void AddingNums()
	{
		int randomIstNum = Random.Range (firstNumMin, firstNumMax);
		int randomI2ndNum = Random.Range (seconfNumMin, secondNumMax);
		Debug.Log ( "random 1" + randomIstNum.ToString());
		Debug.Log ("first num text" + firstNum.text);
		firstNum.text = randomIstNum.ToString();
		secondNum.text = randomI2ndNum.ToString();

		 answer = randomIstNum + randomI2ndNum;
		int randomButtonIndex = Random.Range (0, 4);
		AssignOptions (answer, randomButtonIndex);



		/*button1 = button1.GetComponent<Button> ();
		button1.onClick.AddListener (IsAnsCorrect (answer.ToString()));

		button2 = button1.GetComponent<Button> ();
		button2.onClick.AddListener (IsAnsCorrect (answer.ToString()));

		button3 = button1.GetComponent<Button> ();
		button3.onClick.AddListener (IsAnsCorrect (answer.ToString()));

		button4 = button1.GetComponent<Button> ();
		button4.onClick.AddListener (IsAnsCorrect (answer.ToString()));*/
	}

	private void AssignOptions(int answer, int answerButtonIndex) {
		ButtonClass button;
		for (int i = 0; i < 4; i++) {
			if (i == answerButtonIndex) {
				button = new ButtonClass (answerButtonIndex, answer.ToString());
			} else {
				int r;
				do {
					r = Random.Range (2 * seconfNumMin, 2 * secondNumMax);
					button = new ButtonClass (i, r.ToString ());
				} while (r == answer);
			}
			button.Display ();
		}
	}

	/*public void IsAnsCorrect(string answer)
	{
		if (button1.GetComponentInChildren<Text> ()[0].text == answer.ToString ()
		   || button2.GetComponentInChildren<Text> ()[0].text == answer.ToString ()
		   || button2.GetComponentInChildren<Text> ()[0].text == answer.ToString ()
		   || button2.GetComponentInChildren<Text> ()[0].text == answer.ToString ()) 
		{
			Debug.Log ("correct");;
		}


	}*/




}

public class ButtonClass
{
	private string buttonText;
	private int buttonIndex;

	public ButtonClass(int n, string t)
	{
		this.buttonIndex = n;
		this.buttonText = t;
	}

	public int getButtonIndex()
	{
		return this.buttonIndex;
	}
	public string getButtonText()
	{
		return this.buttonText;
	}
	public string setButtonText(string t)
	{
		return this.buttonText = t;
	}

	public void Display() 
	{
		GameObject buttonObj = GameObject.Find ("MCQ" + (buttonIndex+1)+"Button");
		buttonObj.GetComponentsInChildren<Text> () [0].text = this.buttonText;
	}
}