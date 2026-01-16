using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class UIEventHandler : MonoBehaviour
{
    public static bool gameIsPaused = false;

    public GameObject pauseMenuUI;
    public GameObject playerUI;
    public GameObject menuButtons;
    public GameObject exitDialogPanel;


    void Update()
    {
        if (Input.GetKeyDown(KeyCode.Escape))
        {
            if (gameIsPaused)
            {
                Resume();
            }
            else
            {
                Pause();
            }
        }
    }

    public void Pause()
    {
        pauseMenuUI.SetActive(true);
        menuButtons.SetActive(true);
        playerUI.SetActive(false);
        Time.timeScale = 0f;
        gameIsPaused = true;
    }

    public void Resume()
    {
        pauseMenuUI.SetActive(false);
        playerUI.SetActive(true);
        exitDialogPanel.SetActive(false);
        Time.timeScale = 1f;
        gameIsPaused = false;
    }

    public void Exit()
    {
        // ExitDialogManager exitDialogManager = new ExitDialogManager();
        // exitDialogManager.ShowDialog();
        exitDialogPanel.SetActive(true);
        menuButtons.SetActive(false);

    }

    public void ExitToMainMenu()
    {
        SceneLoadManager sceneLoadManager = new SceneLoadManager();
        sceneLoadManager.LoadTargetScene("MainMenu");
        Time.timeScale = 1f;
    }

    public void ExitToDesktop()
    {
        Application.Quit();
        Debug.Log("went to desktop");
        // Time.timeScale = 1f;
    }

    public void BackToPauseMenu()
    {
        pauseMenuUI.SetActive(true);
        menuButtons.SetActive(true);
        exitDialogPanel.SetActive(false);
    }
}