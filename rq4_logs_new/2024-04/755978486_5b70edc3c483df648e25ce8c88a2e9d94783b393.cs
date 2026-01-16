using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class HackButtonUI : MonoBehaviour
{
    public bool Clicked;

    private Image image;
    private Button button;

    private void Awake()
    {
        image = GetComponent<Image>();
        button = GetComponent<Button>();
    }

    public void SetButton(bool clicked)
    {
        if (!Clicked && clicked)
        {
            HackEventManager.Instance.buttonAmount -= 1;
        }
        Clicked = clicked;
        if (clicked)
        {
            image.color = button.colors.pressedColor;
        }
        else
        {
            image.color = button.colors.normalColor;
        }
        
    }
}