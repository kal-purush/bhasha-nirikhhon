using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Button_levels : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.Backspace))
        {
            Application.LoadLevel("LoadScenes");
        }
    }

    private void OnMouseDown()
    {
        switch (gameObject.name)
        {
            case "1":
                {
                    Application.LoadLevel("GlassMaze");
                    break;
                }
            case "2":
                {
                    Application.LoadLevel("IceMaze");
                    break;
                }
            case "3":
                {
                    Application.LoadLevel("WaterMaze");
                    break;
                }
            case "4":
                {
                    Application.LoadLevel("CripMaze");
                    break;
                }
            case "5":
                {
                    Application.LoadLevel("AliseMaze");
                    break;
                }
            case "menu":
                {
                    Application.LoadLevel("LoadScenes");
                    break;
                }

        }
    }
}