using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;
public class GameOver : MonoBehaviour
{
    public Text currentScore, highScore;
    public void exitToMainMenu()//onclick close
    {
        SceneManager.LoadScene(0);
        Time.timeScale = 1;
    }

    public void restartGame()//onclick restart
    {
        SceneManager.LoadScene(1);
        Time.timeScale = 1;
    }

    void Start()
    {
        //Get current game score
        float cntScore = ScoreManager.s;
        print(cntScore);
        currentScore.text = cntScore.ToString();

        //show highscore
        float hScore = highscoreManager.getHighScore();
        highScore.text = hScore.ToString();

        //replace highscore if current score is greater than high score
        if (cntScore > hScore)
        {
            highscoreManager.setHighScore(cntScore);
            highScore.text = "New \nHigh Score! \n" + cntScore;
        }
        else
            highScore.text = highscoreManager.getHighScore().ToString();
    }
}