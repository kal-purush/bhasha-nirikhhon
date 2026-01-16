using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class GameController : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void setScene(int i)
    {
        if (i == 0) { 
            SceneManager.LoadScene("Level1"); 
        }else if (i == 1)
        {
            SceneManager.LoadScene("Prototype");
        }
    }
}