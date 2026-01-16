using UnityEngine;
using System.Collections;
using System;
using UnityEngine.SceneManagement;

public class OptionalAtmospheresUI : MonoBehaviour {

    private Rect windowRect = new Rect(30,20,325,150);

    private void OnGUI()
    {
        windowRect = GUI.Window(0, windowRect, WindowFunction, "OptionalAtmosphere Manager");
    }

    private void WindowFunction(int WindowID)
    {
        if (GUI.Button(new Rect (30, 50, 80, 20), "Add"))
        {
            SceneManager.LoadScene(1);
        }

        if (GUI.Button(new Rect(210, 50, 80, 20), "Remove"))
        {
            SceneManager.LoadScene(2);
        }

        if(GUI.Button(new Rect(150,30,80,20), "Select Body"))
        {
            selectorRect = GUI.Window(1, selectorRect, BodySelector, "Body Selector");
        }
    }

}