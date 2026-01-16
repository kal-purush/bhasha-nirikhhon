using UnityEngine;
using System.Collections;

public class Crosshair : MonoBehaviour
{
    public Texture2D crosshairTexture;
    public Rect position;
    public static bool OriginalOn = true;

    void Start()
    {
        position = new Rect((Screen.width - crosshairTexture.width) / 2, (Screen.height - crosshairTexture.height) / 2, crosshairTexture.width / 2, crosshairTexture.height / 2);
    }

    void OnGUI()
    {
        //Run/ADS/Idle controller
        if (!PlayerController.ADS)
        {
            OriginalOn = true;
        }
        else
        {
            OriginalOn = false;
        }

        if (OriginalOn == true)
        {
            GUI.DrawTexture(position, crosshairTexture);
        }
    }
}