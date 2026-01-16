using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;


public class MainMenu : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }
    public void play()
    {
        int currentSceneIndex = SceneManager.GetActiveScene().buildIndex;


        SceneManager.LoadScene(currentSceneIndex + 1);
    }
    public void quit()
    {
        Application.Quit(); 
    }
}