using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class GameOverScreen : MonoBehaviour {

    public static int score = 0;
    public Text dispText;

    private void Start() {
        dispText.text = "Your score: " + score;
    }

    public void BackToMenu() {
        SceneManager.LoadScene("MainMenu");
    }

}