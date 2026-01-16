using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class MainMenu : MonoBehaviour
{

    // Code referenced from " gamesplusjames"
    //https://www.youtube.com/watch?v=76WOa6IU_s8

    public string levelOne;
    public string levelTwo;
    public string levelThree;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void LevelOne()
    {
        SceneManager.LoadScene(levelOne);
    }

    public void LevelTwo()
    {
        SceneManager.LoadScene(levelTwo);
    }

    public void LevelThree()
    {
        SceneManager.LoadScene(levelThree);
    }

    public void Exit()
    {
        Application.Quit();
        Debug.Log("Exiting application...");
    }
}