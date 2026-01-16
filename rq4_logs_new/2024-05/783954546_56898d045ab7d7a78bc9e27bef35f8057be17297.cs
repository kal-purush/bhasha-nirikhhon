using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class Win : MonoBehaviour
{
    private string winScence = "Assets/Scenes/WinScreen.unity";
    public GameObject boss;
    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        if (boss == null)
        {
            SceneManager.LoadScene(winScence);
        }
    }
}