using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ScoreUI : MonoBehaviour
{
    // Start is called before the first frame update
    int score;
    public Score scoreScript;

    string displayText = "Score: 0";
    public Text scoreText;
    void Start(){
        scoreText.text = displayText;
    }
    // Update is called once per frame
    void Update()
    {
        score = scoreScript.getScore();
        displayText = "Score: " + score.ToString();
        scoreText.text = displayText;
    }

}