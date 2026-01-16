using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class AnaMenu : MonoBehaviour
{
    public Text HighScore;
    public Text score;


    void Start()
    {
        int enYuksekSkor = PlayerPrefs.GetInt("enYuksekPuanKayit");
        int puan = PlayerPrefs.GetInt("puanKayit");


        HighScore.text = "High Score : " + enYuksekSkor.ToString();
        
        score.text = "Score : " + puan.ToString();
    }

    void Update()
    {

    }
    public void oyunaGit()
    {
        SceneManager.LoadScene("SampleScene");
    }
    public void oyundanCık()
    {
        Application.Quit();
    }
}