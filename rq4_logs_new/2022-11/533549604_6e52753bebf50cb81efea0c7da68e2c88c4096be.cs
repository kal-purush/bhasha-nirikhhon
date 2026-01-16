using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using TMPro;

public class DefenceGameOver : MonoBehaviour
{
    [SerializeField] private GameObject reticle, timerText, coinsText;
    [SerializeField] private TextMeshProUGUI gameOverTimer;
    [SerializeField] private DefenceScoreboard dfsb;
    private DefenceScoreboardEntryData newEntry = new DefenceScoreboardEntryData();
    private bool timeAdded;
    private void Awake()
    {
        if (!timeAdded)
        {
            dfsb.AddEntry(newEntry = new DefenceScoreboardEntryData() {entryTime = DefenceTimer.timer});
            timeAdded = true;
        }
        reticle.SetActive(false);
        Cursor.visible = true;
        timerText.SetActive(false);
        coinsText.SetActive(false);
        gameOverTimer.text = DefenceTimer.timer.ToString("n2");
    }

    public void TryAgain()
    {
        SceneManager.LoadScene(2);
    }

    public void QuitToMainMenu()
    {
        SceneManager.LoadScene(0);
    }
}