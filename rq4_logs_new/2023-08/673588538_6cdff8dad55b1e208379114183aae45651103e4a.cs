using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HighScoreboard : MonoBehaviour
{
    private ScoreBoardNumber[] digits;
    // Holds the score split into an array. The number will be backwards,
    // so the ones place will be stored in scoreArray[0]
    private int[] scoreArray = new int[] {1,2,3,4,5,6};

    // Start is called before the first frame update
    void Start()
    {
        // Populate the digits array
        digits = GetComponentsInChildren<ScoreBoardNumber>();
    }

    // Update is called once per frame
    void Update()
    {
        UpdateScore();
        SetDigits();
    }

    void SetDigits()
    {
        for (int i = 0; i < scoreArray.Length; i++){
            digits[i].SetNumber(scoreArray[i]);
        }
    }

    void UpdateScore()
    {
        int score = GameManager.Instance.HighScore;

        for (int i = 0; i < 6; i++){
            scoreArray[i] = score % 10;
            score = score / 10;
        }
    }


}