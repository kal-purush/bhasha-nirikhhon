using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class GameOverController : MonoBehaviour
{
    [SerializeField] GameObject gameOver;
    [SerializeField] PlayerController player;
    void Start()
    {
        gameOver.SetActive(false);
    }
    public void GameOver()
    {
        gameOver.SetActive(true);
        player.GetComponent<PlayerController>().enabled = false;
    }
    public void RestartLevel()
    {
        HeartController.hearts = 3;
        SceneManager.LoadScene(SceneManager.GetActiveScene().buildIndex);
    }
    public void MainMenu()
    {
        SceneManager.LoadScene(0);
    }
}