using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ColourSizeSwitch : MonoBehaviour
{

    public GameObject PanelColour;
    public GameObject PanelSize;


    public void OpenColour()
    {
        bool isActive = PanelColour.activeSelf;
        PanelColour.SetActive(!isActive);

        PanelSize.SetActive(false);
    }
    public void OpenSize()
    {
        bool isActive = PanelSize.activeSelf;
        PanelSize.SetActive(!isActive);

        PanelColour.SetActive(false);
    }



}