using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class inventoryUI : MonoBehaviour
{
    public Transform itemParent;
    protected inventorySlot[] slots;
    Inventory inventory;
    void Start()
    {
        inventory = Inventory.instance;
        inventory.onItemChangedCallBack += UpdateUI;
        slots = itemParent.GetComponentsInChildren<inventorySlot>();
    }

    // Update is called once per frame
    void Update()
    {
        
    }
    public void UpdateUI()
    {
        for (int i = 0; i < slots.Length; ++i)
        {
            if (i < inventory.inventoryItems.Count)
            {
                slots[i].AddItem(inventory.inventoryItems[i]);
            }
            else
            {
                slots[i].ClearSlot();
            }
        }
    }
}