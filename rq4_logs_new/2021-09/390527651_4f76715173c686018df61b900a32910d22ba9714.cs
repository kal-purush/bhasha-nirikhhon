using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EquippedItem : MonoBehaviour
{
    private SpriteRenderer activeSprite;
    private Item activeItem;

    void Start()
    {
        activeSprite = this.GetComponent<SpriteRenderer>();
    }

    public void updateActiveItem(Item newItem)
    {
        if (newItem.itemTemplate.isEquippable)
        {
            activeSprite.sprite = newItem.GetSprite();
            activeItem = newItem;
        }
        else
        {
            activeSprite.sprite = null;
            activeItem = null;
        }
    }
}