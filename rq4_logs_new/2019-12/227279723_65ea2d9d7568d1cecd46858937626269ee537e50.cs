using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class MenuManager : MonoBehaviour
{
    private bool paused = false;
    [SerializeField] private Text m_countdownText = null;
    [SerializeField] private float m_pauseDuration = 3f;
    private float timer = 0;
    private bool gameStarting = false;

    // Start is called before the first frame update
    void Start()
    {
    }

    // Update is called once per frame
    void Update()
    {
        if (!paused && timer > 0)
        {
            timer -= Time.unscaledDeltaTime;
            m_countdownText.text = timer.ToString("0");
        }

        if (!paused && timer <= 0)
        {
            Time.timeScale = 1;
            m_countdownText.text = "";
            gameObject.SetActive(false);
        }

    }

    public void PauseGame()
    {
        paused = !paused;

        if (paused)
            Time.timeScale = 0;
        else
        {
            gameStarting = true;
            timer = m_pauseDuration;
        }
    }

    public void StartGame()
    {
        Time.timeScale = 0;
        paused = false;
        timer = m_pauseDuration;
    }

    public void ExitGame()
    {
        Application.Quit();
    }
}