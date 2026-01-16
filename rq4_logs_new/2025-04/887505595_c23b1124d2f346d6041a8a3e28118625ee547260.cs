using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class TuringMemoryCard : MonoBehaviour
{
    [SerializeField] private Image iconImage;

    public Sprite hiddenIcon;
    public Sprite visibleIcon;

    public bool isSelected;
    public TuringMemoryController controller;


    public void OnCardClick()
    {
        controller.SetSelected(this);
    }

    public void SetIconSprite(Sprite icon)
    {
        visibleIcon = icon;
    }

    public void Show()
    {
        iconImage.sprite = visibleIcon;
        isSelected = true;
    }

    public void Hide()
    {
        iconImage.sprite = hiddenIcon;
        isSelected = false;
    }

}