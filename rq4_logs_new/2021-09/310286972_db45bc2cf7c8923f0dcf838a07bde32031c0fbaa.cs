using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class TitleGameStart_M : MonoBehaviour
{
    public GameObject gameMode;
    public string cleared, sceneName;

    void Start()
    {
        cleared = PlayerPrefs.GetString("storyClear");
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void OnClick()
    {
        if (cleared == "clear")
        {
            gameMode.SetActive(true);
        }
        else
        {
            SceneManager.LoadScene(sceneName);
        }
    }
}