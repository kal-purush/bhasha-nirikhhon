using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class GameController : MonoBehaviour
{
    
    //Variables
    public int health = 10;
    public int level = 1;

    void Start()
    {
         if(PlayerPrefs.HasKey("PlayerHealth"))
        {
            health = PlayerPrefs.GetInt("PlayerHealth");
        }
        else
        {
            PlayerPrefs.SetInt("PlayerHealth", health);
        }
        if(PlayerPrefs.HasKey("CurrentLevel"))
        {
            level = PlayerPrefs.GetInt("CurrentLevel");
        }
        else
        {
            PlayerPrefs.SetInt("CurrentLevel", level);
        }
    }

    // Update is called once per frame
    void Update()
    {
        if (health <= 0)
        {
            GameOver();
        }
    }
    private void ChangeLevel()
    {
        PlayerPrefs.SetInt("PlayerHealth", health);
        PlayerPrefs.SetInt("CurrentLevel", level + 1);
        SceneManager.LoadScene(SceneManager.GetActiveScene().buildIndex);
    }
    public void GameOver()
    {
        Debug.Log("GameOver!!");
        //Solution to game over
        #if UNITY_EDITOR
        UnityEditor.EditorApplication.isPlaying = false;
#else
        Application.Quit();
#endif
    }

    private void OnApplicationQuit()
    {
        PlayerPrefs.DeleteKey("PlayerHealth");
        PlayerPrefs.DeleteKey("CurrentLevel");
    }
}