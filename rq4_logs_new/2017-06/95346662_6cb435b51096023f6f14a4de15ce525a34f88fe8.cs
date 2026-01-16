using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class Main_menu : MonoBehaviour 
{
    public bool isStart;
    public bool isOptions;
    public bool isQuit;

	void Start () 
    {
	}

    void OnMouseUp()
    {
        if (isStart)
        {
            SceneManager.LoadScene("Level");
        }

        if (isOptions)
        {
            SceneManager.LoadScene("Options");
        }

        if (isQuit)
        {
            Application.Quit();
        }
    } 

	void Update () 
    {
	}
}