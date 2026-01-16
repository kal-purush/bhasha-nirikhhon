using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ScoreManager : MonoBehaviour
{
    public static int tempScore;
    public Text score;
    public float _100ms, nxtMS, baseScore = 0, finalScore;
    public static float s;

    void Awake()
    {
        _100ms = 0.1f; nxtMS = 0.0f;//Rate of which base score is added

        // Reset the score.
        tempScore = 0;
    }

    // Update is called once per frame
    void Update()
    {
        finalScore = tempScore;
        //Base score
        if (Time.time > nxtMS)//0.1 second to add 1 to score
        {
            nxtMS = Time.time + _100ms;
            baseScore++;         

        }
        finalScore += baseScore;
        score.text = "" + finalScore;   //display score

        s = finalScore;
    }
}