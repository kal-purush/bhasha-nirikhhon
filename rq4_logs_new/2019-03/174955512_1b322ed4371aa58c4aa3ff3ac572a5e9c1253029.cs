using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ScoreBoard : MonoBehaviour
{
    [SerializeField]
    Text text;
    int score;
    int scoreToDisplay;
    // Start is called before the first frame update
    void Start()
    {
        SetScore(50000);
    }

    void UpdateNumber(){
        text.text = scoreToDisplay.ToString();
        if(score <= scoreToDisplay) {
            scoreToDisplay = score;
            return;
        }
        scoreToDisplay += 87;
    }

    void SetScore(int val){
        score = val;
    }

    // Update is called once per frame
    void Update()
    {
        UpdateNumber();
    }
}