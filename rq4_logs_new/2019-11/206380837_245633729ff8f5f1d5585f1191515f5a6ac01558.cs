using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class SettingsMenu : MonoBehaviour
{

    public GameObject settingsMenu;

    public void Start()
    {

        AudioListener.volume = PlayerPrefs.GetFloat("volume");

    }

    public void onClick()
    {
        Toggle();

    }

    public void Toggle()
    {
        settingsMenu.SetActive(!settingsMenu.activeSelf);

        if (settingsMenu.activeSelf)
        {
            Time.timeScale = 0f;
        }
        else
        {
            Time.timeScale = 1f;
        }
    }

}