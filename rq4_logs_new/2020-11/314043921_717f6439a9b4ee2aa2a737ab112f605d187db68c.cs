using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class ManageScenes : MonoBehaviour
{

	public void MainMenuScene ()
    {
       SceneManager.LoadScene("MainMenu");
    }

    public void ScanScene ()
    {
       SceneManager.LoadScene("Scan");
    }

    public void OptionsScene ()
    {
       SceneManager.LoadScene("Options");
    }

    public void QuitApp()
    {
        Debug.Log("QUIT!");
        Application.Quit();
    }
}