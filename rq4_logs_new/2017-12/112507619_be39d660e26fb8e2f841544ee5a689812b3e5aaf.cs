using System.Collections;
using System.Collections.Generic;
using UnityEngine;

// Loading SceneManagement
using UnityEngine.SceneManagement;
public class SceneController : MonoBehaviour {


    public static SceneController instance;

	// Use this for initialization
	void Start () {
	    if( instance == null)
        {
            instance = this;
            DontDestroyOnLoad(gameObject);
        }
        else
        {
            Destroy(gameObject);
        }
	}
	
	// Update is called once per frame
	void Update () {
		
        // Cheat Load Main Menu
        if (Input.GetKeyDown(KeyCode.F5))
        {
            SceneManager.LoadScene("Master");
        }

        // load next scene
        if(Input.GetKeyDown(KeyCode.F2))
        {
            int index = SceneManager.GetActiveScene().buildIndex + 1;

            if( index < SceneManager.sceneCountInBuildSettings)
            {
                SceneManager.LoadScene(index); 
            }
            else
            {
                SceneManager.LoadScene(0);
            }
        }
        // load previous scene
        if (Input.GetKeyDown(KeyCode.F1))
        {
            int index = SceneManager.GetActiveScene().buildIndex - 1;

            if (index > 0)
            {
                SceneManager.LoadScene(index);
            }
            else
            {
                SceneManager.GetActiveScene();
            }

            // Quit to the end screen
            if (Input.GetKeyDown(KeyCode.F12))
            {
                SceneManager.LoadScene("Game Over");
            }
        }
	}
}