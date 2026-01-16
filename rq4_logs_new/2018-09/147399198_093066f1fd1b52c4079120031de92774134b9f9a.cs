using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ClickedScript : MonoBehaviour
{
    public bool clickedIs = false;

    public void OnClick()
    {
        clickedIs = true;
    }

    public void OnStopClick()
    {
        clickedIs = false;
    }
}