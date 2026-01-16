using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ItemStorage : MonoBehaviour
{
    public List<ItemInventory> items = new List<ItemInventory>();
    public InventoryRenderer inventoryRenderer;
    public float maxWeight;

    public void SetItems(List<ItemInventory> newItems)
    {
        items = newItems;
    }

    public List<ItemInventory> GetItems()
    {
        return items;
    }

    void Start()
    {
        while (items.Count < 36)
        {
            ItemInventory item = new ItemInventory();
            item.id = 0;
            item.count = 0;
            items.Add(item);
        }
    }


    // добавляем предмет класса item в инвентарь
    public void AddItem(int id, Item item, int count)
    {
        items[id].id = item.id;
        items[id].count = count;
        items[id].itemGameObj.GetComponent<Image>().sprite = item.img;

        if (count > 1 && item.id != 0)
        {
            items[id].itemGameObj.GetComponentInChildren<Text>().text = count.ToString();
        }
        else
        {
            items[id].itemGameObj.GetComponentInChildren<Text>().text = "";
        }
    }

    // добавляем предмет класса iteminventory в инвентарь
    public void AddInventoryItem(int id, ItemInventory invItem)
    {
        items[id].id = invItem.id;
        items[id].count = invItem.count;
        items[id].itemGameObj.GetComponent<Image>().sprite = inventoryRenderer.data.items[invItem.id].img;

        if (invItem.count > 1 && invItem.id != 0)
        {
            items[id].itemGameObj.GetComponentInChildren<Text>().text = invItem.count.ToString();
        }
        else
        {
            items[id].itemGameObj.GetComponentInChildren<Text>().text = "";
        }
    }
}

[System.Serializable]

public class ItemInventory
{
    public int id;
    public GameObject itemGameObj;
    public int count;
}