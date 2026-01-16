using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ScoreTracker : MonoBehaviour {

  public int leftScore;
  public Text leftText;
  public Goal leftGoal;

  public int rightScore;
  public Text rightText;
  public Goal rightGoal;

	// Use this for initialization
	void Start () {
    leftGoal.callback += ScoreLeft;
    rightGoal.callback += ScoreRight;
    leftText.text = "0";
    rightText.text = "0";
	}
	
	// Update is called once per frame
	void Update () {
		
	}

  void ScoreLeft() {
    leftScore += 1;
    leftText.text = leftScore.ToString();
  }

  void ScoreRight() {
    rightScore += 1;
    rightText.text = rightScore.ToString();
  }
}