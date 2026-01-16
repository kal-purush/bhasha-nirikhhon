using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class ButtonQuit : MonoBehaviour
{
    private void OnMouseUpAsButton(){
        Debug.Log("Exit button was pressed!");
        Application.Quit();
        Debug.Log("Game is exiting.");
    }
}
