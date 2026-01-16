using Inventory.Model;
using NUnit.Framework;
using System.Collections.Generic;
using UnityEngine;

//chứa tụi edible Item
public class EdibleManager : Singleton<EdibleManager>
{
    public List<EdibleItemSO> edibleItem = new List<EdibleItemSO>();

    public ItemSO GetEdibleItem(int itemID)
    {
        for (int i = 0; i < edibleItem.Count; i++)
        {
            if (edibleItem[i].ItemID == itemID) return edibleItem[i];
        }
        return null;
    }

    [ContextMenu("Cheat InventoryData")]
    void CheatInventory()
    {
        InventoryData inventoryDatas = DBController.Instance.INVENTORY_DATA;
        inventoryDatas.SetSlotData(1, 0, 3);
        inventoryDatas.SetSlotData(7, 1, 4);
        DBController.Instance.INVENTORY_DATA = inventoryDatas;
    }

    [ContextMenu("Cheat Reset InventoryData")]
    void CheatResetInventory()
    {
        InventoryData inventoryDatas = DBController.Instance.INVENTORY_DATA;
        inventoryDatas.ResetSlotDataIndex(1);
        DBController.Instance.INVENTORY_DATA = inventoryDatas;
    }
}