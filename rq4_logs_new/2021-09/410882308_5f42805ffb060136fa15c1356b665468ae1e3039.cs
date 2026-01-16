using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;

public class HighScore : MonoBehaviour
{
    public TextMeshProUGUI highscore1;
    public TextMeshProUGUI highscore2;
    public TextMeshProUGUI highscore3;
    public TextMeshProUGUI highscore4;
    public TextMeshProUGUI highscore5;
    void Start()
    {
        highscore1.text = "1 - " + PlayerPrefs.GetString("HighName1") + " : " + PlayerPrefs.GetInt("HighScore1");
        highscore2.text = "2 - " + PlayerPrefs.GetString("HighName2") + " : " + PlayerPrefs.GetInt("HighScore2");
        highscore3.text = "3 - " + PlayerPrefs.GetString("HighName3") + " : " + PlayerPrefs.GetInt("HighScore3");
        highscore4.text = "4 - " + PlayerPrefs.GetString("HighName4") + " : " + PlayerPrefs.GetInt("HighScore4");
        highscore5.text = "5 - " + PlayerPrefs.GetString("HighName5") + " : " + PlayerPrefs.GetInt("HighScore5");
    }
}