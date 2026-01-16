using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class InventoryManager : MonoBehaviour
{
    public Image[] inventorySlots; // ?????????

    // ??????????
    public void AddItem(Sprite itemSprite, int slotIndex)
    {
        if (slotIndex >= 0 && slotIndex < inventorySlots.Length)
        {
            inventorySlots[slotIndex].sprite = itemSprite;
            inventorySlots[slotIndex].enabled = true; // ????????
            inventorySlots[slotIndex].gameObject.SetActive(true);
        }
    }

    // ?????????
    public void RemoveItem(int slotIndex)
    {
        if (slotIndex >= 0 && slotIndex < inventorySlots.Length)
        {
            inventorySlots[slotIndex].sprite = null; // ????
            inventorySlots[slotIndex].enabled = false; // ????????
            inventorySlots[slotIndex].gameObject.SetActive(false);
        }
    }
}