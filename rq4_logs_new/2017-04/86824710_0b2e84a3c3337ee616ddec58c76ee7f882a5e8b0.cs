using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class Gate : MonoBehaviour
{
    
   
    void OnTriggerEnter2D()
    {
       SceneManager.LoadScene("Nuuksio");
        Debug.Log("Entering nuuksio"); 
    }

    
}
    