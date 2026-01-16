using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MainMenu : MonoBehaviour
{
    //This class contains the methods used in the main menu for buttons.
    public void Full_Screen(bool isFullScreen)
    {
        Screen.fullScreen = isFullScreen;
        Debug.Log("Full Screen is " + isFullScreen);
    }
    public void ExitButton()
    {
        Application.Quit();
        Debug.Log("Game has closed");
    }
    public void PlayButton()
    {
        UnityEngine.SceneManagement.SceneManager.LoadScene("Home");
    }
}