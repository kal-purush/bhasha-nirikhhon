using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;
using TMPro;

public class ButtonScript : MonoBehaviour
{
    public GameObject map;
    private TMP_Text text;

    void Start()
    {
       text = map.GetComponent<TMP_Text>();
    }
    public void ChangeScene()
    {
        if(text.text == "Map 1")
        {
            SceneManager.LoadScene(1);
        }
        else if(text.text == "Map 2")
        {
            SceneManager.LoadScene(1);
        }
        else if(text.text == "Map 3")
        {
            SceneManager.LoadScene(2);
        }
        
    }
}