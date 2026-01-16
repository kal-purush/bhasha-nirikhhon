using System;
using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class GameManager : MonoBehaviour
{
    #region Singletone
    public static GameManager Instance;
    private void Awake()
    {
        Instance = this;
    }
    #endregion

    public int Score;
    public List<GameObject> Lifes;
    public TextMeshProUGUI scoreText;
    

    public void AddScore(int score)
    {
        Score += score;
        scoreText.text = "Score : " +
                         Environment.NewLine +Score;
    }

    public void UpdateLife()
    {
        Debug.Log(Lifes.Count-1);
       Lifes[Lifes.Count-1].SetActive(false);
       Lifes.Remove(Lifes[Lifes.Count-1]);
    }
}