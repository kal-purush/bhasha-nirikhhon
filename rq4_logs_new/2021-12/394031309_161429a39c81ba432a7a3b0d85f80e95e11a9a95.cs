using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class MainMenuManager : MonoBehaviour
{
    [SerializeField] Text HighScoreStr;
    int highScore;

    // Start is called before the first frame update
    void Start()
    {
        if (PlayerPrefs.HasKey("HighScore"))
        {
            highScore = PlayerPrefs.GetInt("HighScore");
            HighScoreStr.text = highScore.ToString();
        }
        else
        {
            highScore = 0;
            HighScoreStr.text = "0";
        }
        
    }

    public void Button_Play()
    {
        SceneManager.LoadScene(1);
    }
   
}