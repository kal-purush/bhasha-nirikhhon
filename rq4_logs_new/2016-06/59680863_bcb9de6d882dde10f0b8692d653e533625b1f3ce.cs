using UnityEngine;
using UnityEngine.UI;
using System.Collections;
using System.Collections.Generic;
using UnityEngine.SceneManagement;

public class GameMaster : MonoBehaviour {
    public Text countdownText;
    public PlayerController player;
    public EnemyAI[] enemies;
    public float timeLeft = 3f;

    private bool playedSound = false;

    public void restartGame() {
        SceneManager.LoadScene("Menu");
    }

	void FixedUpdate() {
        if(timeLeft <= -2)
            return;
        timeLeft -= Time.fixedDeltaTime;
        if(timeLeft <= -1f) {
            countdownText.text = "";
        } else if(timeLeft <= 0f) {
            countdownText.text = "START";
            if(!playedSound) {
                playedSound = true;
                GetComponent<AudioSource>().Play();
                player.run = true;
                foreach(EnemyAI eAI in enemies) {
                    eAI.run = true;
                }
            }
        } else if(timeLeft <= 1f) {
            countdownText.text = "1";
        } else if(timeLeft <= 2f) {
            countdownText.text = "2";
        } else if(timeLeft <= 3f) {
            countdownText.text = "3";
        }
    }
}