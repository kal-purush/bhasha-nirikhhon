using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ScreenSet : MonoBehaviour
{
    [SerializeField]
    Vector2Int screenSize = new Vector2Int(1280, 720);
    // Start is called before the first frame update
    void Start()
    {
        //if (Screen.fullScreenMode == FullScreenMode.Windowed)
        {
            Screen.SetResolution(screenSize.x, screenSize.y, FullScreenMode.Windowed);
        }
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}