using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class doorConditions : MonoBehaviour
{
    private bool showText = false;

    // when door is clicked on, show text
    void OnMouseDown(){
        
        if (!showText){           
            showText = true;
        }
    }

    void OnGUI(){
        // if it's locked, show text that it's locked
        if (showText && gameObject.GetComponent<GameManager>().locked){
            if (GUI.Button(new Rect(100, 100, 100, 20), "Door is Locked")){
                showText = false;
            }
        } else if (showText && !gameObject.GetComponent<GameManager>().locked){
            if (GUI.Button(new Rect(100, 100, 100, 20), "You've Escaped!")){
                showText = false;
            }
        }
    }
}