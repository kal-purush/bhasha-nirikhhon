using UnityEngine;
using UnityEngine.SceneManagement;

public class PauseGame : MonoBehaviour
{
    public GameObject pauseMenu;
    public bool isPaused;
    public GameObject settingPanel;
    bool isSetting;
    public GameObject ctrlPanel;
    bool isCtrl;

    MouseLook mouseLook;

    private void Start()
    {
        isPaused = false;
        pauseMenu.SetActive(false);
        Time.timeScale = 1;
        mouseLook = FindObjectOfType<MouseLook>();
    }

    private void Update()
    {
        Pause();
    }

    public void Pause()
    {
        if (isSetting && !isCtrl)
        {
            if (Input.GetKeyDown(KeyCode.Escape))
            {
                HideSetting();
            }
        }
        else if (isCtrl)
        {
            if (Input.GetKeyDown(KeyCode.Escape))
            {
                
                HideCtrl();
            }
        }
        else if (Input.GetKeyDown(KeyCode.Escape))
        {
            if (isPaused)
            {
                Resume();
            }
            else
            {
                isPaused = true;
                pauseMenu.SetActive(true);
                Time.timeScale = 0;
                mouseLook.ShowMouse();
            }
        }
    }

    public void Resume()
    {
        isPaused = false;
        pauseMenu.SetActive(false);
        Time.timeScale = 1;
        mouseLook.HideMouse();
    }

    public void MainMenu()
    {
        Time.timeScale = 1;
        SceneManager.LoadScene("MainMenu");
    }

    public void ShowSetting()
    {
        settingPanel.SetActive(true);
        isSetting = true;
    }
    public void HideSetting()
    {
        settingPanel.SetActive(false);
        isSetting = false;
    }

    public void ShowCtrl()
    {
        ctrlPanel.SetActive(true);
        isCtrl = true;
    }
    public void HideCtrl()
    {
        ctrlPanel.SetActive(false);
        isCtrl = false;
    }
}