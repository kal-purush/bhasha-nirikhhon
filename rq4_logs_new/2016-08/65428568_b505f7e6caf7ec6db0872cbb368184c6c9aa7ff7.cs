using UnityEngine;
using System.Collections;
using System;
using UnityEngine.SceneManagement;
using UnityEngine.UI;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

public class  LoseMenu : GenericMenu
{

    public Text highScoreText;

    // Use this for initialization
    new void Start()
    {

        Debug.Log("global score: " + ScoreKeeper.global_score);
        SetSubtitle("Score: " + ScoreKeeper.global_score);
      
        SetHighScore("High Score: " + ScoreKeeper.highScore);
     
        base.Start();
    }

    private void SetHighScore(string v)
    {
        highScoreText.text = v;
    }
}