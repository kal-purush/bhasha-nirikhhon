using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class MenuButtons : MonoBehaviour
{
    private GameObject creditsPanel;
    bool creditsActive = false;

    // Start is called before the first frame update
    void Start()
    {
        creditsPanel = GameObject.Find("pnlCredits");
        creditsPanel.SetActive(false);
    }

    public void ToggleCredits()
    {
        creditsActive = !creditsActive;
        creditsPanel.SetActive(creditsActive);
    }

    public void LoadScene(string sceneName)
    {
        SceneManager.LoadScene(sceneName);
    }

    public void ExitGame()
    {
        Application.Quit();
    }
}