using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EndSceneScript : MonoBehaviour
{	
    void Update()
    {
        if (Input.GetButtonDown("Submit"))
        {
                Application.LoadLevel(0);
        }
    }
       
}