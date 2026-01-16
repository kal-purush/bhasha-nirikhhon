using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
public class LobbyController : MonoBehaviour
{
    [SerializeField] GameObject MainMenu;
    [SerializeField] GameObject Options;
    public void StartGame()
    {
        SceneManager.LoadScene(SceneManager.GetActiveScene().buildIndex + 1);
    }
    public void OpenOptions()
    {
        MainMenu.SetActive(false);
        Options.SetActive(true);
    }
    public void BackToMenu()
    {
        MainMenu.SetActive(true);
        Options.SetActive(false);
    }
    public void QuitGame()
    {
        Application.Quit();
    }
}