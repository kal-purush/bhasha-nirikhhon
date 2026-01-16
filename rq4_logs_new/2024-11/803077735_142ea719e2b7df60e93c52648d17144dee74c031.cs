using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.UI;

public class ArmourSlotHolder : MonoBehaviour
{
    // Variables
    public Image image;
    public GameObject inventoryItemPrefab;

    /// <summary>
    /// This returns the item stored in the slot if there is one.
    /// </summary>
    /// <returns>null if there's no item stored or the Item that's stored.</returns>
    public Item GetStoredItem()
    {
        if (transform.childCount == 0)
        {
            return null;
        }
        else
        {
            return gameObject.GetComponentInChildren<InventoryItem>().storedItem;
        }
    }

    /// <summary>
    /// This returns the current stack size of the item in the slot.
    /// </summary>
    /// <returns>An integer specifying the current stack size. Returns 0 if there's no item in the slot.</returns>
    public int GetCurrentStackSize()
    {
        if (transform.childCount != 0)
        {
            return gameObject.GetComponentInChildren<InventoryItem>().currentStackSize;
        }
        return 0;
    }

    /// <summary>
    /// This adds the object to the slot if it is empty.
    /// </summary>
    /// <param name="eventData"></param>
    public void OnDrop(PointerEventData eventData)
    {
        if (transform.childCount == 0)
        {
            InventoryItem draggableItem = eventData.pointerDrag.GetComponent<InventoryItem>();
            draggableItem.parentAfterDrag = transform;

            InventoryManager.instance.EquipItem(draggableItem.storedItem);
        }
    }
}