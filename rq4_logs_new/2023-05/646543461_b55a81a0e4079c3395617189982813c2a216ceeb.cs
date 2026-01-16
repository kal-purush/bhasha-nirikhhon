using System.Collections;
using System.Collections.Generic;
using UnityEngine;

#if UNITY_EDITOR
using UnityEditor;
#endif

public class GameControl : MonoBehaviour
{
    [SerializeField] private GameData data;
    [SerializeField] private GameObject pauseMenu;
    
    public bool isPaused;

    private void Awake()
    {
        data.ResetData();
        pauseMenu.SetActive(false);
        isPaused = false;

        Cursor.visible = false;
        Cursor.lockState = CursorLockMode.Locked;
    }

    private void Update()
    {
        data.gameTime += Time.deltaTime;
        
        if (Input.GetKeyDown(KeyCode.Escape))
        {
            if (isPaused)
            {
                pauseMenu.SetActive(false);
                isPaused = false;
                Time.timeScale = 1f;
            }
            else
            {
                pauseMenu.SetActive(true);
                isPaused = true;
                Time.timeScale = 0f;
            }
        }

        if (Input.GetKeyDown(KeyCode.Q) && isPaused)
        {
            QuitApplication();
        }
    }

    void QuitApplication()
    {
        #if UNITY_EDITOR
        EditorApplication.ExitPlaymode();
        #else
        Application.Quit();
        #endif
    }
}