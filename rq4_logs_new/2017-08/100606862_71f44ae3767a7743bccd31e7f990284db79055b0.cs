using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Navigator : MonoBehaviour {

    public ToggleButton ranking;
    public ToggleButton premios;
    public ToggleButton beertender;
    public ToggleButton contacto;

    public void TurnOffAll()
    {
        ranking.Toggle(false);
        premios.Toggle(false);
        beertender.Toggle(false);
        contacto.Toggle(false);
    }

    public void BTN_Ranking()
    {
        ranking.Toggle(true);
    }

    public void BTN_Premios()
    {
        premios.Toggle(true);
    }

    public void BTN_Beertender()
    {
        beertender.Toggle(true);
    }

    public void BTN_Contacto()
    {
        contacto.Toggle(true);
    }



}

[System.Serializable]
public class ToggleButton
{
    public Button button;
    public Sprite on;
    public Sprite off;

    public void Toggle(bool active)
    {
        button.image.sprite = (active) ? on : off;
    }

}