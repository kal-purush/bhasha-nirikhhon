using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.UI;

public class ButtonHoverScript : MonoBehaviour, IPointerEnterHandler, IPointerExitHandler
{
    public Sprite hoverSprite; // Sprite to show when the cursor is over the button
    private Sprite originalSprite; // Original sprite of the button

    private Image buttonImage;

    private void Start()
    {
        buttonImage = GetComponent<Image>();
        originalSprite = buttonImage.sprite;
    }

    public void OnPointerEnter(PointerEventData eventData)
    {
        // Change the button's sprite when the cursor enters it
        buttonImage.sprite = hoverSprite;
    }

    public void OnPointerExit(PointerEventData eventData)
    {
        // Restore the original sprite when the cursor exits the button
        buttonImage.sprite = originalSprite;
    }
}
