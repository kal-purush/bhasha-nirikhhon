using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
public class LoadMainMenu : MonoBehaviour
{
    GameData gameData;
    private void Start()
    {
        gameData = GameObject.Find("GameData").GetComponent<GameData>();
        if (gameData is null)
        {
            Debug.Log("Cannot find game data");
        }
    }


    public void LoadMenu()
    {
        gameData.lastSceneOpened = "Win_scene";
        SceneManager.LoadScene("MainMenu");
        
    }
}